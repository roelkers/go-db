package pager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"github.com/roelkers/go_db/pkg/row"
)

type Pager struct {
  pageSize int
  file *os.File
  pages map[int]Page
  rowNumber int
  maxPages int
}

func (p * Pager) clearCache() {
  p.pages = make(map[int]Page)
}

func (p * Pager) PageMap() map[int]Page {
  return p.pages
}

func (p * Page) Rows() [] row.Row {
  return p.rows
}

type Page struct {
  rows []row.Row
  size int
}

func NewPager(filename string, pageSize int, maxPages int) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_CREATE | os.O_RDWR, 0600)
  if err != nil {
    return nil,err
  }
  pages := make(map[int]Page)
  pager := Pager{
    pageSize: pageSize,
    file: file,
    pages: pages,
    maxPages: maxPages,
  }
  
  return &pager,nil
}

func (p * Pager) GetPage(pageNumber int) (*Page,error) {
  //cache hit
  if item,ok := p.pages[pageNumber]; ok {
    return &item,nil
  }
  //cache miss
  filePos := int64(pageNumber * p.pageSize)
  fmt.Println(filePos)
  p.file.Seek(filePos, io.SeekStart)
  scanner, _ := row.NewScanner(p.file, p.pageSize)
  rows := make([]row.Row,0)
  bytesRead := 0
  for scanner.Scan() && (bytesRead + scanner.Row().Size() < p.pageSize) {
    row := scanner.Row()
    bytesRead += row.Size()
    rows = append(rows, *row)
  }
	if scanner.Err() != nil {
		return nil, scanner.Err()
	}
  p.pages[pageNumber] = Page {
    rows:rows,
  }
  item := p.pages[pageNumber]
  return &item,nil
}

func (p * Pager) FlushPages() error {
    for i := range p.pages {
      err := p.flushPage(i)
      if err != nil {
        return err
      }
    }
    return nil
}

func (p * Pager) flushPage(pageNumber int) (error) {
  if _,ok := p.pages[pageNumber]; !ok {
    return errors.New("Page not in cache. Cannot be flushed") 
  }
  _, err := p.file.Seek(int64(pageNumber * p.pageSize), io.SeekStart)
  if err != nil {
    return err
  }
  bytes := make([]byte,0)
  page := p.pages[pageNumber]
  for _,r := range page.rows {
    bytes = append(bytes, r.ToBytes()...)
  }
  if len(bytes) > p.pageSize{
    return errors.New("Length rows in page bigger than page size")
  }
  n, err := p.file.Write(bytes)
  if err != nil {
    return err
  }
  fmt.Printf("FlushPage: Written %d bytes \n", n)
  err = p.file.Sync()
  if err != nil {
    return err
  }
  return nil
}

func (p * Pager) getPageWithSpace(r * row.Row) (int, error) {
    newRowSize := len(r.ToBytes())
    for i:= 0; i <= p.maxPages; i++ {
      page, err := p.GetPage(i)
      if err != nil {
        return 0, err
      }
      if page.size + newRowSize <= p.pageSize {
        return i,nil 
      }
    }
    return 0, errors.New("getPageWithSpace: Out of pages")
}

func (p * Pager) AppendRow(r * row.Row) (error) {
    pageNr, err := p.getPageWithSpace(r)
    if err != nil {
      return err
    }
    page := p.pages[pageNr]
    newSize := len(r.ToBytes()) + page.size
    newPage := Page {
      rows : append(page.rows, *r),
      size : newSize,
    }
    p.rowNumber++
    p.pages[pageNr] = newPage
    return nil
}

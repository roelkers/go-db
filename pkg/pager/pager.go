package pager

import (
	"errors"
	"io"
	"os"
	"github.com/roelkers/go_db/pkg/row"
)

type Pager struct {
  pageSize int
  file *os.File
  pages []Page
  rowNumber int
  maxPages int
}

func (p * Pager) clearCache() (error){
  stat,err := p.file.Stat()
  if err != nil {
    return err
  }
  fileSize := stat.Size()
  nrPagesToLoad :=  fileSize / int64(p.pageSize)
  p.pages = make([]Page, nrPagesToLoad)
  return err
}

func (p * Pager) Pages() []Page {
  return p.pages
}

func (p * Page) Rows() [] row.Row {
  return p.rows
}

type Page struct {
  rows []row.Row
  size int
  loaded bool
}

func minInitPageNr(nrPagesToLoad int64) int64 {
  if nrPagesToLoad == 0 {
    return 1
  } 
  return nrPagesToLoad
}

func NewPager(filename string, pageSize int, maxPages int) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_CREATE | os.O_RDWR, 0600)
  if err != nil {
    return nil,err
  }
  stat,err := file.Stat()
  fileSize := stat.Size()
  nrPagesToLoad :=  fileSize / int64(pageSize) 
  initPageNr := minInitPageNr(nrPagesToLoad)
  pages := make([]Page, initPageNr)
  pager := Pager{
    pageSize: pageSize,
    file: file,
    pages: pages,
    maxPages: maxPages,
  }
  for i := 0; i <= int(nrPagesToLoad); i++ {
    _, err := pager.GetPage(i)
    if err != nil {
      return nil,err
    }
  }
  
  return &pager,nil
}

func (p * Pager) hasPageAndIsLoaded(pageNumber int) bool {
  if len(p.pages) <= pageNumber {
    return false
  }
  item := p.pages[pageNumber]
  if item.loaded {
    return true
  }
  return false
}

func (p * Pager) GetPage(pageNumber int) (*Page,error) {
  //cache hit
  if p.hasPageAndIsLoaded(pageNumber) {
    item := p.pages[pageNumber]
    return &item,nil
  }
  //This is an assumption, that we only need to increase the pages by one
  //Actually would be better to resize the slice up to page number
  if(len(p.pages) <= pageNumber) {
    p.pages = append(p.pages, Page{})
  } 
  //cache miss
  filePos := int64(pageNumber * p.pageSize)
  p.file.Seek(filePos, io.SeekStart)
  scanner, _ := row.NewScanner(p.file, p.pageSize)
  rows := make([]row.Row,0)
  bytesRead := 0
  for scanner.Scan() && (bytesRead + scanner.Row().Size() <= p.pageSize) {
    row := scanner.Row()
    bytesRead += row.Size()
    rows = append(rows, *row)
  }
  //fmt.Printf("GetPage: Read %d bytes\n", bytesRead)
	// if scanner.Err() != nil {
	// 	return nil, scanner.Err()
	// }
  p.pages[pageNumber] = Page {
    rows:rows,
    loaded: true,
    size: bytesRead,
  }
  //item = p.pages[pageNumber]
  return &p.pages[pageNumber],nil
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
  if pageNumber > len(p.pages) {
    return errors.New("Page not in cache. Index too large") 
  }
  item := p.pages[pageNumber]
  if !item.loaded {
    return errors.New("Page not in cache. Not loaded") 
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
  _, err = p.file.Write(bytes)
  if err != nil {
    return err
  }
  //fmt.Printf("FlushPage: Written %d bytes \n", n)
  err = p.file.Sync()
  if err != nil {
    return err
  }
  return nil
}

func (p * Pager) getPageWithSpace(r * row.Row) (*Page, error) {
    newRowSize := len(r.ToBytes())
    for i:= 0; i <= p.maxPages; i++ {
      page, err := p.GetPage(i)
      if err != nil {
        return nil, err
      }
      if page.size + newRowSize <= p.pageSize {
        return &p.pages[i],nil 
      } 
    }
    return nil, errors.New("getPageWithSpace: Out of pages")
}

func (p * Pager) AppendRow(r * row.Row) (error) {
    page, err := p.getPageWithSpace(r)
    if err != nil {
      return err
    }
    newSize := len(r.ToBytes()) + page.size
    page.rows = append(page.rows, *r)
    page.size = newSize
    p.rowNumber++
    return nil
}

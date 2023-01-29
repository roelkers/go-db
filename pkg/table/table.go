package table

import (
	"errors"
	"fmt"
	"os"
  "bytes"

	"github.com/roelkers/go_db/pkg/row"
)


const (
  PREPARE_SUCCESS = iota 
  PREPARE_UNRECOGNIZED_STATEMENT 
  PREPARE_SYNTAX_ERROR
)

type StatementType int

const (
  STATEMENT_INSERT StatementType = iota
  STATEMENT_SELECT
)

const (
  PAGE_SIZE = 1024
)

type Page struct {
  bytes []byte
}

///TABLE
// var TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

type Table struct {
  pages []Page
  rowNr int
}


func MakeTable() (* Table) {
  pages := make([]Page,1)
	table := Table{
    pages: pages,
    rowNr: 0,
	}	
	return &table
}

type Statement struct {
  typ StatementType
  row *row.Row
}

func (t * Table) DoMetaCommand(cmd string) (error) {
  if cmd == ".exit" {
    os.Exit(0)
    return errors.New("Exit failed")
  } else {
    return errors.New("Unrecognized command")
  }
}

func (t * Table) PrepareStatement(cmd string, statement* Statement) (error) {
  if cmd[:6] == "select" {
    statement.typ = STATEMENT_SELECT
    return nil
  }
  if cmd[:6] == "insert" {
    statement.typ = STATEMENT_INSERT
    var (
      username string
      email string
      id uint32
    ) 
    argsAssigned, err := fmt.Sscanf(cmd, "insert %d %s %s", &id, &username, &email)
    if err != nil {
      return err
    }
    if(argsAssigned < 3) {
      return errors.New("Syntax Error")
    }
    statement.row = row.NewRow(id, username, email) 
    return nil
  }
  return errors.New("Unrecognized statement")
}

func (t * Table) executeInsert(statement* Statement) {
    bytes := statement.row.ToBytes() 
    pageNr := len(t.pages)
    currPageBytes := t.pages[pageNr-1].bytes
    byteNr := len(currPageBytes)
    if(byteNr + len(bytes) > PAGE_SIZE) {
      t.pages = append(t.pages, Page{})
      pageNr += 1
      currPageBytes = t.pages[pageNr-1].bytes
    }
   t.pages[pageNr-1].bytes = append(currPageBytes, bytes...)
   t.rowNr += 1
}

func (t * Table) scanPage(page Page) []row.Row {
  reader := bytes.NewReader(page.bytes)
  scanner, _ := row.NewScanner(reader, 4096)
  rows := make([]row.Row, 0)
  for scanner.Scan() {
    rows = append(rows, *scanner.Row())
  }
  return rows
}

func (t * Table) executeSelect(statement* Statement) (error) {
  for _,page := range t.pages {
     fmt.Println("New page boundary")
     rows := t.scanPage(page)
     for i,row := range(rows) {
       fmt.Printf("row %d, username: %s, email: %s\n", i, row.Username(), row.Email())
     } 
  }
  return nil
}

func (t * Table) ExecuteStatement(statement* Statement) {
  switch(statement.typ) {
    case STATEMENT_INSERT:
      t.executeInsert(statement)
      return
    case STATEMENT_SELECT:
      t.executeSelect(statement)
      return
  }
}

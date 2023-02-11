package table

import (
	"errors"
	"fmt"
	"os"

	"github.com/roelkers/go_db/pkg/row"
	"github.com/roelkers/go_db/pkg/pager"
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

///TABLE
// var TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

type Table struct {
  pager *pager.Pager
}

func MakeTable(filename string, pageSize int, maxPages int) (* Table, error) {
  pager, err := pager.NewPager(filename, pageSize, maxPages) 
  if err != nil {
    return nil,err
  }
	table := Table{
    pager: pager,
	}	
	return &table,nil
}

type Statement struct {
  typ StatementType
  row *row.Row
}

func (t * Table) Pager() (* pager.Pager) {
  return t.pager
}

func (t * Table) DoMetaCommand(cmd string) (error) {
  if cmd == ".exit" {
    err := t.pager.FlushPages()
    if err != nil {
      return err
    }
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
  t.pager.AppendRow(statement.row)
}

func (t * Table) executeSelect(statement* Statement) (error) {
  // fmt.Println(p.Rows()[0].Email())
  for i := range t.pager.Pages() {
     fmt.Printf("New page boundary page nr is %d\n", i)
     page,err := t.pager.GetPage(i)
     if err != nil {
       return err
     }
     for n,r := range page.Rows() {
       fmt.Printf("row %d, username: %s, email: %s\n", n, r.Username(), r.Email())
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

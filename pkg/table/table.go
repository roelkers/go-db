package table

import (
	"errors"
	"fmt"
	"os"

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


type Page struct {
  rows [] row.Row 
}

///TABLE
// var TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

type Table struct {
  pages []Page
}


func MakeTable() (* Table) {
	table := Table{
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
  if cmd == "insert" {
    statement.typ = STATEMENT_INSERT
    var (
      username string
      email string
      id uint32
    ) 
    argsAssigned, _ := fmt.Sscanf(cmd, "insert %d %s %s", id, username, email)
    if(argsAssigned < 3) {
      return errors.New("Syntax Error")
    }
    statement.row = row.NewRow(id, username, email) 
    return nil
  }
  return  errors.New("Unrecognized statement")
}

func (t * Table) ExecuteStatement(statement* Statement) {
  switch(statement.typ) {
    case STATEMENT_INSERT:
    case STATEMENT_SELECT:
  }
}

func (t * Table) executeInsert(statement* Statement) {
}

func (t * Table) executeSelect(statement* Statement) {
}

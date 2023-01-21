package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const (
  META_COMMAND_SUCCESS = iota 
  META_COMMAND_UNRECOGNIZED_SUCCESS 
)

const (
  PREPARE_SUCCESS = iota 
  PREPARE_UNRECOGNIZED_STATEMENT 
)


type StatementType int

const (
  STATEMENT_INSERT StatementType = iota
  STATEMENT_SELECT
)

type Statement struct {
  typ StatementType
}

func doMetaCommand(cmd string) int {
  if cmd == ".exit" {
    os.Exit(0)
    return META_COMMAND_SUCCESS
  } else {
    return META_COMMAND_UNRECOGNIZED_SUCCESS
  }
}

func prepareStatement(cmd string, statement* Statement) int {
  if cmd[:6] == "select" {
    statement.typ = STATEMENT_SELECT
    return PREPARE_SUCCESS
  }
  if cmd == "insert" {
    statement.typ = STATEMENT_INSERT
    return PREPARE_SUCCESS
  }
  return PREPARE_UNRECOGNIZED_STATEMENT
}

func executeStatement(statement* Statement)  {
  switch(statement.typ) {
    case STATEMENT_INSERT:
    case STATEMENT_SELECT:
  }
}

func main() {
  reader := bufio.NewReader(os.Stdin)
  fmt.Println("Please enter a database command")
  for {
    text, _ := reader.ReadString('\n');
    text = strings.TrimSuffix(text, "\n")
    if text[0] == '.' {
      switch doMetaCommand(text) {
        case META_COMMAND_SUCCESS:
          continue
        case META_COMMAND_UNRECOGNIZED_SUCCESS:
          fmt.Printf("Unrecognized command %s \n", text)
      }
    }
    statement := new(Statement)
    switch prepareStatement(text, statement) {
        case PREPARE_SUCCESS:
        case PREPARE_UNRECOGNIZED_STATEMENT: 
          fmt.Printf("Unrecognized keyword at start of '%s'.\n", text)
          continue
    }

    executeStatement(statement)
    fmt.Println("Executed")
  }
}

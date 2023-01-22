package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"hash/crc32"
	"encoding/binary"
	"errors"
)

//Errors
var ErrInsufficientData = errors.New("could not parse bytes")
var ErrCorruptData = errors.New("the record has been corrupted")

/// ROWS
const (
  CRCLEN_SIZE = 4 
  ID_SIZE = 4
  USERNAMELEN_SIZE = 4 
  EMAILLEN_SIZE = 4 
	META_LENGTH = USERNAMELEN_SIZE + EMAILLEN_SIZE + CRCLEN_SIZE + ID_SIZE
)

type Row struct {
  id uint32
  username string
  email string
}

//var ROW_SIZE = unsafe.Sizeof(exampleRow)

///PAGES
// const (
//   PAGE_SIZE = 4096
//   TABLE_MAX_PAGES = 100
//   // ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
// )

type Page struct {
  rows [] Row
}

///TABLE
// var TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

type Table struct {
  pages []Page
}

///STATEMENTS
const (
  META_COMMAND_SUCCESS = iota 
  META_COMMAND_UNRECOGNIZED_SUCCESS 
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

type Statement struct {
  typ StatementType
  row Row
}

func makeTable() (* Table) {
	table := Table{
	}	
	return &table
}

//Conversion
// ToBytes serializes the record into a sequence of bytes
func (r *Row) ToBytes() []byte {
	usernameBytes := []byte(r.username)
	usernameLen := make([]byte, USERNAMELEN_SIZE)
	binary.BigEndian.PutUint32(usernameLen, uint32(len(usernameBytes)))

	emailLen := make([]byte, EMAILLEN_SIZE)
	binary.BigEndian.PutUint32(emailLen, uint32(len(r.email)))
	idBytes := make([]byte, ID_SIZE)
	binary.BigEndian.PutUint32(idBytes, r.id)

	data := []byte{}
	crc := crc32.NewIEEE()
	for _, v := range [][]byte{idBytes, usernameLen, emailLen, []byte(r.username), []byte(r.email)} {
		data = append(data, v...)
		crc.Write(v)
	}

	crcData := make([]byte, CRCLEN_SIZE)
	binary.BigEndian.PutUint32(crcData, crc.Sum32())
	return append(crcData, data...)
}

// FromBytes deserialize []byte into a record. If the data cannot be
// deserialized a wrapped ErrParse error will be returned.
func FromBytes(data []byte) (*Row, error) {
	if len(data) < META_LENGTH {
		return nil, ErrInsufficientData
	}

	idStart := CRCLEN_SIZE;
	userNameLenStart := CRCLEN_SIZE + ID_SIZE
	emailLenStart := CRCLEN_SIZE + ID_SIZE + USERNAMELEN_SIZE
	idb := data[idStart: idStart+ID_SIZE]
	ulb := data[userNameLenStart: userNameLenStart+ USERNAMELEN_SIZE]
	elb := data[emailLenStart : emailLenStart+EMAILLEN_SIZE]

	crc := uint32(binary.BigEndian.Uint32(data[:4]))
	id := uint32(binary.BigEndian.Uint32(idb))
	usernameLen := int(binary.BigEndian.Uint32(ulb))
	emailLen := int(binary.BigEndian.Uint32(elb))

	if len(data) < META_LENGTH+emailLen+usernameLen {
		return nil, ErrInsufficientData
	}

	usernameStartIdx := META_LENGTH
	emailStartIdx := usernameStartIdx + usernameLen

	username := make([]byte, usernameLen)
	email := make([]byte, emailLen)
	copy(username, data[usernameStartIdx:emailStartIdx])
	copy(email , data[emailStartIdx:emailStartIdx+emailLen])

	check := crc32.NewIEEE()
	check.Write(data[4 : META_LENGTH+usernameLen+emailLen])
	if check.Sum32() != crc {
		return nil, ErrCorruptData
	}

	return &Row{id: id, username: string(username), email: string(email)}, nil
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
    argsAssigned, _ := fmt.Sscanf(cmd, "insert %d %s %s", &statement.row.id, &statement.row.username, &statement.row.email)
    if(argsAssigned < 3) {
      return PREPARE_SYNTAX_ERROR
    }
    return PREPARE_SUCCESS
  }
  return PREPARE_UNRECOGNIZED_STATEMENT
}

func (t * Table) executeStatement(statement* Statement) {
  switch(statement.typ) {
    case STATEMENT_INSERT:
    case STATEMENT_SELECT:
  }
}

func (t * Table) executeInsert(statement* Statement) {
}

func (t * Table) executeSelect(statement* Statement) {
}

func main2() {
	table := makeTable()
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

    table.executeStatement(statement)
    fmt.Println("Executed")
  }
}

func main() { 
	row := Row{
		id: 1,
		username: "rufus",
		email: "rufus.oelkers@gmail.com",
	}
	bytes := row.ToBytes()
	readRow, err := FromBytes(bytes)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(readRow.username)
	fmt.Println(readRow.email)
	fmt.Println(readRow.id)
}

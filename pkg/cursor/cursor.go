package cursor

import (
	// "errors"
	// "io"
	// "os"
	"github.com/roelkers/go_db/pkg/table"
	// "github.com/roelkers/go_db/pkg/row"
)

type Cursor struct {
  table *table.Table
	rowNumber int
}

func tableStartCursor(table *table.Table) (* Cursor) {
  cursor := Cursor {
		table: table,
		rowNumber: 0,
	} 
	return &cursor
}

func tableWithSpaceCursor(table *table.Table) (* Cursor) {
  pageNr, err := table.pager.getPageWithSpace(r)
	return &cursor
}


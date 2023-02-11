package cursor

import (
	"github.com/roelkers/go_db/pkg/table"
	"github.com/roelkers/go_db/pkg/row"
)

type Cursor struct {
  table *table.Table
	rowNumber int
	pageNumber int
}

func TableStartCursor(t *table.Table) (* Cursor, error) {
  cursor := Cursor {
		table: t,
		rowNumber: 0,
		pageNumber: 0,
	} 
	return &cursor, nil
}

func TableEndCursor(t *table.Table) (* Cursor, error) {
	nrPages := len(t.Pager().Pages())
	pageNr := nrPages-1
  page, err:= t.Pager().GetPage(pageNr)
	if err != nil {
		return nil, err
	}
	// last row
	rowNumber := len(page.Rows())-1
  cursor := Cursor {
		table: t,
		rowNumber: rowNumber,
		pageNumber: pageNr,
	} 
	return &cursor,nil
}

func (cursor *Cursor) HasNext() (bool, error) {
	// is not last row in last page
 page, err:= cursor.table.Pager().GetPage(cursor.pageNumber)
	if err != nil {
		return false,err
	}
  if cursor.rowNumber < len(page.Rows())  {		
		return true, nil
	}
	nrPages := len(cursor.table.Pager().Pages())
	// is not last page
  if cursor.pageNumber < nrPages {
	  return true, nil
	}
	return false, nil
}

func (cursor *Cursor) Next() (error) {
	// if item is last in the page, advance the page counter
  page, err:= cursor.table.Pager().GetPage(cursor.pageNumber)
	if err != nil {
		return err
	}
	// increase row
  if cursor.rowNumber < len(page.Rows()) {		
		cursor.rowNumber += 1 
		return nil
	}
	// if row can't be increased, next page
	nrPages := len(cursor.table.Pager().Pages())
	//if pn = 9, inc when nrp = 10
  if cursor.pageNumber < nrPages  {
		cursor.pageNumber += 1
		cursor.rowNumber = 0
	}
	return nil
}

func (cursor *Cursor) Value() (* row.Row, error) {
  page, err:= cursor.table.Pager().GetPage(cursor.pageNumber)
	if err != nil {
		return nil, err
	}
	r := page.Rows()[cursor.rowNumber]
	return &r,nil
}

func (cursor *Cursor) AppendRow(r* row.Row) (error) {
  return cursor.table.Pager().AppendRow(r)
}

package pager

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/roelkers/go_db/pkg/row"
)

func TestAppendOnePage(t *testing.T) {
	PAGE_SIZE := 440
	NROWS := 10
	r := row.NewRow(1, "rufus.oelkers@yahoo.com", "rufus")
	p,err := NewPager("./table.db", PAGE_SIZE, 1)
	require.NoError(t, err)
	for i:= 0; i < NROWS; i++ {
		err := p.AppendRow(r)
		require.NoError(t, err)
  }
	rows := p.GetPage(0).rows
	for i:= 0; i < NROWS; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
}

func TestAppendMultiPage(t *testing.T) {
	PAGE_SIZE := 440 
	NROWS := 30
	NROWS_P_PAGE := NROWS / 3
	r := row.NewRow(1, "rufus.oelkers@yahoo.com", "rufus")
	p,err := NewPager("./table.db", PAGE_SIZE, 3)
	require.NoError(t, err)
	for i:= 0; i < NROWS; i++ {
		err := p.AppendRow(r)
		require.NoError(t, err)
  }
	rows := p.GetPage(0).rows
	for i:= 0; i < NROWS_P_PAGE; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
	rows = p.GetPage(1).rows
	for i:= 0; i < NROWS_P_PAGE; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
	rows = p.GetPage(2).rows
	for i:= 0; i < NROWS_P_PAGE; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
}

func TestInsertMany(t *testing.T) {
	tab,err := MakeTable("./table.db" , 10)
	require.NoError(t, err)
	for i:= 0; i < 200; i++ {
		r := row.NewRow(uint32(i), "rufus.oelkers@gmail.com", "rufus")
		s := Statement{
			typ: STATEMENT_INSERT,
			row: r,
		}
		tab.executeInsert(&s)
	}
	s := Statement{
		typ: STATEMENT_SELECT,
	}
	err = tab.executeSelect(&s)
	require.NoError(t, err)
}

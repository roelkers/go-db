package table

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/roelkers/go_db/pkg/row"
)

func TestInsertSelect(t *testing.T) {
	row := row.NewRow(1, "rufus.oelkers@gmail.com", "rufus")
	tab,err := MakeTable("./table.db", 100, 10)
	require.NoError(t, err)
	s := Statement{
		typ: STATEMENT_INSERT,
		row: row,
	}
	tab.executeInsert(&s)
	s = Statement{
		typ: STATEMENT_SELECT,
	}
	err = tab.executeSelect(&s)
	require.NoError(t, err)
}

func TestInsertMany(t *testing.T) {
	tab,err := MakeTable("./table.db" , 10, 10)
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

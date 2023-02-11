package cursor

import (
	"testing"
	"io/ioutil"
	"os"
	"path"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/roelkers/go_db/pkg/row"
	"github.com/roelkers/go_db/pkg/table"
)

func TestCursorOnePage(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()

	NROWS := 3 
	tab,err := table.MakeTable(path.Join(testPath, "table.db"), 440, 10)
	require.NoError(t, err)
	for i:= 0; i < NROWS; i++ {
		r := row.NewRow(1, fmt.Sprintf("rufus.oelker%d@yahoo.com", i), "rufus")
		err := tab.Pager().AppendRow(r)
		require.NoError(t, err)
  }
	cursor,err := TableStartCursor(tab)
	err = cursor.Next()
	require.NoError(t, err)
	err = cursor.Next()
	require.NoError(t, err)
	cursorRow,err := cursor.Value()
	require.NoError(t, err)
	r := row.NewRow(1, fmt.Sprintf("rufus.oelker%d@yahoo.com", 2), "rufus")

  require.Equal(t, r.Email(), cursorRow.Email())
  require.Equal(t, r.Id(), cursorRow.Id())
  require.Equal(t, r.Username(), cursorRow.Username())
}

func TestCursorMultiPage(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()

	NROWS := 30 
	tab,err := table.MakeTable(path.Join(testPath, "table.db"), 440, 10)
	require.NoError(t, err)
	for i:= 0; i < NROWS; i++ {
		r := row.NewRow(1, fmt.Sprintf("rufus.oelker%d@yahoo.com", i % 10), "rufus")
		err := tab.Pager().AppendRow(r)
		require.NoError(t, err)
  }
	cursor,err := TableStartCursor(tab)
	for ok:= true; ok == true; ok,err = cursor.HasNext() {
		cursor.Next()
	}
	require.NoError(t, err)
	r := row.NewRow(1, fmt.Sprintf("rufus.oelker%d@yahoo.com", 9), "rufus")

	cursorRow,err := cursor.Value()
  require.Equal(t, r.Email(), cursorRow.Email())
  require.Equal(t, r.Id(), cursorRow.Id())
  require.Equal(t, r.Username(), cursorRow.Username())
}

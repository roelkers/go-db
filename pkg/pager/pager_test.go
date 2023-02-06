package pager

import (
	"testing"
	"io/ioutil"
	"os"
	"path"
	"github.com/stretchr/testify/require"
	"github.com/roelkers/go_db/pkg/row"
)

func TestAppendOnePage(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()

	PAGE_SIZE := 440
	NROWS := 10
	r := row.NewRow(1, "rufus.oelkers@yahoo.com", "rufus")
	p,err := NewPager(path.Join(testPath, "table.db"), PAGE_SIZE, 1)
	require.NoError(t, err)
	for i:= 0; i < NROWS; i++ {
		err := p.AppendRow(r)
		require.NoError(t, err)
  }
	page,err := p.GetPage(0)
	require.NoError(t, err)
	rows := page.rows
	for i:= 0; i < NROWS; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
}

func TestAppendMultiPage(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()
	PAGE_SIZE := 440 
	NROWS := 30
	NROWS_P_PAGE := NROWS / 3
	r := row.NewRow(1, "rufus.oelkers@yahoo.com", "rufus")
	p,err := NewPager(path.Join(testPath, "table.db"), PAGE_SIZE, 3)
	require.NoError(t, err)
	for i:= 0; i < NROWS; i++ {
		err := p.AppendRow(r)
		require.NoError(t, err)
  }
	page,err := p.GetPage(0)
	require.NoError(t, err)
	rows := page.rows
	for i:= 0; i < NROWS_P_PAGE; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
	page, err = p.GetPage(1)
	require.NoError(t, err)
	rows = page.rows
	for i:= 0; i < NROWS_P_PAGE; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
	page,err = p.GetPage(2)
	require.NoError(t, err)
	rows = page.rows
	for i:= 0; i < NROWS_P_PAGE; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
}

func TestGetAndFlushPage(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()

	PAGE_SIZE := 4096
	NROWS := 10
	r := row.NewRow(1, "rufus.oelkers@yahoo.com", "rufus")
	p,err := NewPager(path.Join(testPath, "table.db"), PAGE_SIZE, 1)
	require.NoError(t, err)
	for i:= 0; i < NROWS; i++ {
		err := p.AppendRow(r)
		require.NoError(t, err)
  }
	err = p.FlushPages()
	require.NoError(t, err)
	p.clearCache()
	page,err := p.GetPage(0)
	require.NoError(t, err)
	rows := page.rows
	for i:= 0; i < NROWS; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
}

func TestGetAndFlushMultiPage(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()
	PAGE_SIZE := 440 
	NROWS := 30
	NROWS_P_PAGE := NROWS / 3
	r := row.NewRow(1, "rufus.oelkers@yahoo.com", "rufus")
	p,err := NewPager(path.Join(testPath, "table.db"), PAGE_SIZE, 3)
	require.NoError(t, err)
	for i:= 0; i < NROWS; i++ {
		err := p.AppendRow(r)
		require.NoError(t, err)
  }

	err = p.FlushPages()
	require.NoError(t, err)
	p.clearCache()

	page,err := p.GetPage(0)
	require.NoError(t, err)
	rows := page.rows
	for i:= 0; i < NROWS_P_PAGE; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
	page, err = p.GetPage(1)
	require.NoError(t, err)
	rows = page.rows
	for i:= 0; i < NROWS_P_PAGE; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
	page,err = p.GetPage(2)
	require.NoError(t, err)
	rows = page.rows
	for i:= 0; i < NROWS_P_PAGE; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }

}

func TestGetAndFlushMultiPageNotAligned(t *testing.T) {
	testPath, err := ioutil.TempDir("./", "test")
	if err != nil {
		t.Fatal(err)
	}

	defer (func() {
		os.RemoveAll(testPath)
	})()
	PAGE_SIZE := 500 
	NROWS := 30
	NROWS_P_PAGE := NROWS / 3
	r := row.NewRow(1, "rufus.oelkers@yahoo.com", "rufus")
	p,err := NewPager(path.Join(testPath, "table.db"), PAGE_SIZE, 3)
	require.NoError(t, err)
	for i:= 0; i < NROWS; i++ {
		err := p.AppendRow(r)
		require.NoError(t, err)
  }

	err = p.FlushPages()
	require.NoError(t, err)
	p.clearCache()

	page,err := p.GetPage(0)
	require.NoError(t, err)
	rows := page.rows
	for i:= 0; i < NROWS_P_PAGE+1; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
	page, err = p.GetPage(1)
	require.NoError(t, err)
	rows = page.rows
	for i:= 0; i < NROWS_P_PAGE+1; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }
	page,err = p.GetPage(2)
	require.NoError(t, err)
	rows = page.rows
	for i:= 0; i < 8; i++ {
		require.Equal(t, rows[i].Email(), r.Email())
		require.Equal(t, rows[i].Id(), r.Id())
		require.Equal(t, rows[i].Username(), r.Username())
  }

}

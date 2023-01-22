package row

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func TestFromToBytes(t *testing.T) {
	row := Row{
		id: 1,
		username: "rufus",
		email: "rufus.oelkers@gmail.com",
	}
	bytes := row.ToBytes()
	readRow, err := FromBytes(bytes)
	require.NoError(t, err)
	require.Equal(t, readRow.email, row.email)
	require.Equal(t, readRow.id, row.id)
	require.Equal(t, readRow.username, row.username)
}

package row

import (
	"io"
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

func (row *Row) Id() uint32 {
 return row.id
}

func (row *Row) Username() string {
 return row.username
}

func (row *Row) Email () string {
 return row.email
}

func NewRow(id uint32, username string, email string) (*Row) {
	return &Row{
		id: id,
		username: username,
		email: email,
	}
}

// Size returns the serialized byte size
func (r *Row) Size() int {
	return META_LENGTH + len(r.username) + len(r.email)
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

func (r *Row) Write(w io.Writer) (int, error) {
	data := r.ToBytes()
	return w.Write(data)
}

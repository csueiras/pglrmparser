package lrm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// ErrUnknownType is an error for whenever an unhandled message type is received by the parser
var ErrUnknownType = fmt.Errorf("unknown message type")

var order = binary.BigEndian
var postgresEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// TruncateOption are the options on a Truncate message
type TruncateOption int

const (
	// CascadeTruncateOption indicates that the TRUNCATE executed with the CASCADE option which automatically truncate
	// all tables that have foreign-key references to any of the named tables, or to any tables added to the group due
	// to CASCADE.
	CascadeTruncateOption TruncateOption = 1
	// RestartIdentityTruncateOption indicates that the TRUNCATE executed with the RESTART IDENTITY option which
	// automatically restart sequences owned by columns of the truncated table(s).
	RestartIdentityTruncateOption TruncateOption = 2
)

// LRM is a marker interface for Logical Replication Messages
type LRM interface {
	isLRM()
}

// Insert is an INSERT LRM
type Insert struct {
	// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	NewTuple bool
	// TupleData message part representing the contents of new tuple.
	TupleData []*Tuple
}

func (i Insert) isLRM() {
}

var _ LRM = &Insert{}

// Delete is a DELETE LRM
type Delete struct {
	// Id of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData submessage as a key. This field is present if the table in which the delete
	// has happened uses an index as REPLICA IDENTITY.
	IsKey bool
	// Identifies the following TupleData message as a old tuple. This field is present if the table in which the delete
	// has happened has REPLICA IDENTITY set to FULL.
	IsOld bool
	// TupleData message part representing the contents of the old tuple or primary key, depending on the previous field.
	TupleData []*Tuple
}

func (d Delete) isLRM() {
}

var _ LRM = &Delete{}

// Update is an UPDATE LRM
type Update struct {
	// Id of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData submessage as a key. This field is optional and is only present if the update
	// changed data in any of the column(s) that are part of the REPLICA IDENTITY index.
	IsKey bool
	// Identifies the following TupleData submessage as an old tuple. This field is optional and is only present if
	// table in which the update happened has REPLICA IDENTITY set to FULL.
	IsOld bool
	// TupleData message part representing the contents of the old tuple or primary key. Only present if the previous
	// IsKey or IsOld are set to true.
	TupleData []*Tuple
	// Identifies the following TupleData message as a new tuple.
	IsNew bool
	// TupleData message part representing the contents of a new tuple.
	NewTupleData []*Tuple
}

func (u Update) isLRM() {
}

var _ LRM = &Update{}

// Column is a column in a Relation
type Column struct {
	// Marks the column as part of the key.
	IsKey bool
	// Name of the column.
	Name string
	// ID of the column's data type.
	DataType uint32
	// Type modifier of the column.
	DataTypeModifier uint32
}

// Relation is a table/view LRM
type Relation struct {
	// Id of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Namespace (empty string for pg_catalog).
	Namespace string
	// Relation name.
	Name string
	// Replica identity setting for the relation (same as relreplident in pg_class).
	Replica uint8
	// Columns, won't be present in the case of generated columns
	Columns []*Column
}

func (r Relation) isLRM() {
}

var _ LRM = &Relation{}

// Begin is a BEGIN LRM
type Begin struct {
	// The final LSN of the transaction.
	LSN uint64
	// Commit timestamp of the transaction.
	CommitTime time.Time
	// Xid of the transaction.
	XID uint32
}

func (b Begin) isLRM() {
}

var _ LRM = &Begin{}

// Commit is a COMMIT LRM
type Commit struct {
	// The LSN of the commit.
	LSN uint64
	// The end LSN of the transaction.
	EndLSN uint64
	// Commit timestamp of the transaction.
	CommitTime time.Time
}

func (c Commit) isLRM() {
}

var _ LRM = &Commit{}

// Typ is a data type LRM
type Typ struct {
	// ID of the data type.
	DataTypeID uint32
	// Namespace (empty string for pg_catalog).
	Namespace string
	// Name of the data type.
	Name string
}

func (t Typ) isLRM() {
}

var _ LRM = &Typ{}

// Truncate is a TRUNCATE LRM
type Truncate struct {
	// Number of relations
	NumRelations uint32
	// Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY
	Option TruncateOption
	// ID of the relation corresponding to the ID in the relation message. This field is repeated for each relation.
	ID []uint32
}

func (t Truncate) isLRM() {
}

var _ LRM = &Truncate{}

type parser struct {
	msg *bytes.Buffer
	len int
	ch  byte
}

// Parse parses a Logical Replication Message from the given buffer
func Parse(msg []byte) (LRM, error) {
	p := &parser{
		msg: bytes.NewBuffer(msg),
		len: len(msg),
	}
	return p.readMessage()
}

// readMessage parses an LRM message
func (p *parser) readMessage() (LRM, error) {
	ch, err := p.readNext()
	if err != nil {
		return nil, err
	}

	switch ch {
	case 'B':
		return p.readBegin()
	case 'C':
		return p.readCommit()
	case 'I':
		return p.readInsert()
	case 'D':
		return p.readDelete()
	case 'U':
		return p.readUpdate()
	case 'R':
		return p.readRelation()
	case 'Y':
		return p.readType()
	case 'T':
		return p.readTruncate()
	default:
		return nil, fmt.Errorf("%w: unknown message type identified with=%v", ErrUnknownType, ch)
	}
}

// readInsert parses an INSERT according to the following specs:
//
// Byte1('I')  - Identifies the message as an insert message.
//
// Int32 - ID of the relation corresponding to the ID in the relation message.
//
// Byte1('N') - Identifies the following TupleData message as a new tuple.
//
// TupleData - TupleData message part representing the contents of new tuple.
func (p *parser) readInsert() (*Insert, error) {
	relationID, err := p.readUint32()
	if err != nil {
		return nil, err
	}

	newTuple, err := p.readNext()
	if err != nil {
		return nil, err
	}

	tupleData, err := p.readTupleData()
	if err != nil {
		return nil, err
	}

	return &Insert{
		RelationID: relationID,
		TupleData:  tupleData,
		NewTuple:   newTuple == 'N',
	}, nil
}

// readDelete parses an DELETE according to the following specs:
//
// Byte1('D') - Identifies the message as a delete message.
//
// Int32 - ID of the relation corresponding to the ID in the relation message.
//
// Byte1('K') - Identifies the following TupleData submessage as a key. This field is present if the table in which the
// delete has happened uses an index as REPLICA IDENTITY.
//
// Byte1('O') - Identifies the following TupleData message as a old tuple. This field is present if the table in which the delete has happened has REPLICA IDENTITY set to FULL.
//
// TupleData - TupleData message part representing the contents of the old tuple or primary key, depending on the previous field.
//
// The Delete message may contain either a 'K' message part or an 'O' message part, but never both of them.
func (p *parser) readDelete() (*Delete, error) {
	relationID, err := p.readUint32()
	if err != nil {
		return nil, err
	}

	key, err := p.readNext()
	if err != nil {
		return nil, err
	}

	old, err := p.readNext()
	if err != nil {
		return nil, err
	}

	tupleData, err := p.readTupleData()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return &Delete{
		RelationID: relationID,
		IsKey:      key == 'K',
		IsOld:      old == 'O',
		TupleData:  tupleData,
	}, nil
}

// readUpdate parses an UPDATE according to the following specs:
//
// Byte1('U') - Identifies the message as an update message.
//
// Int32 - ID of the relation corresponding to the ID in the relation message.
//
// Byte1('K') - Identifies the following TupleData submessage as a key. This field is optional and is only present if the update changed data in any of the column(s) that are part of the REPLICA IDENTITY index.
//
// Byte1('O') - Identifies the following TupleData submessage as an old tuple. This field is optional and is only present if table in which the update happened has REPLICA IDENTITY set to FULL.
//
// TupleData - TupleData message part representing the contents of the old tuple or primary key. Only present if the previous 'O' or 'K' part is present.
//
// Byte1('N') - Identifies the following TupleData message as a new tuple.
//
// TupleData - TupleData message part representing the contents of a new tuple.
//
// The Update message may contain either a 'K' message part or an 'O' message part or neither of them, but never both of them.
func (p *parser) readUpdate() (*Update, error) {
	relationID, err := p.readUint32()
	if err != nil {
		return nil, err
	}

	key, err := p.readNext()
	if err != nil {
		return nil, err
	}

	old, err := p.readNext()
	if err != nil {
		return nil, err
	}

	up := &Update{
		RelationID: relationID,
		IsKey:      key == 'K',
		IsOld:      old == 'O',
	}

	if up.IsKey || up.IsOld {
		up.TupleData, err = p.readTupleData()
		if err != nil && err != io.EOF {
			return nil, err
		}
	}

	n, err := p.readNext()
	if err != nil {
		return nil, err
	}

	up.IsNew = n == 'N'
	if up.IsNew {
		up.NewTupleData, err = p.readTupleData()
		if err != nil && err != io.EOF {
			return nil, err
		}
	}
	return up, nil
}

// readRelation parses a relation according to the following specs:
//
// Byte1('R') - Identifies the message as a relation message.
//
// Int32 - ID of the relation.
//
// String - Namespace (empty string for pg_catalog).
//
// String - Relation name.
//
// Int8 - Replica identity setting for the relation (same as relreplident in pg_class).
//
// Int16 - Number of columns.
//
// Next, the following message part appears for each column (except generated columns):
//
// Int8 - Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as part of the key.
//
// String - Name of the column.
//
// Int32 - ID of the column's data type.
//
// Int32 - Type modifier of the column (atttypmod).
func (p *parser) readRelation() (*Relation, error) {
	relationID, err := p.readUint32()
	if err != nil {
		return nil, err
	}
	namespace, err := p.readString()
	if err != nil {
		return nil, err
	}
	name, err := p.readString()
	if err != nil {
		return nil, err
	}
	replica, err := p.readUint8()
	if err != nil {
		return nil, err
	}
	numColumns, err := p.readUint16()
	if err != nil {
		return nil, err
	}

	rel := &Relation{
		RelationID: relationID,
		Namespace:  namespace,
		Name:       name,
		Replica:    replica,
	}

	if numColumns > 0 {
		rel.Columns = make([]*Column, 0, numColumns)
	}

	for c := 0; c < int(numColumns); c++ {
		key, err := p.readNext()
		if err != nil {
			return nil, err
		}
		colName, err := p.readString()
		if err != nil {
			return nil, err
		}
		dataType, err := p.readUint32()
		if err != nil {
			return nil, err
		}
		dataTypeModifier, err := p.readUint32()
		if err != nil {
			return nil, err
		}
		rel.Columns = append(rel.Columns, &Column{
			IsKey:            key == 1,
			Name:             colName,
			DataType:         dataType,
			DataTypeModifier: dataTypeModifier,
		})
	}
	return rel, nil
}

// readBegin parses a BEGIN according to the following specs:
//
// Byte1('B') - Identifies the message as a begin message.
//
// Int64 - The final LSN of the transaction.
//
// Int64 - Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
//
// Int32 - Xid of the transaction.
func (p *parser) readBegin() (*Begin, error) {
	lsn, err := p.readUint64()
	if err != nil {
		return nil, err
	}
	commitTime, err := p.readTimestamp()
	if err != nil {
		return nil, err
	}
	xid, err := p.readUint32()
	if err != nil {
		return nil, err
	}
	return &Begin{
		LSN:        lsn,
		CommitTime: commitTime,
		XID:        xid,
	}, nil
}

// readCommit parses a COMMIT according to the following specs:
//
// Byte1('C') - Identifies the message as a commit message.
//
// Int8 - Flags; currently unused (must be 0).
//
// Int64 - The LSN of the commit.
//
// Int64 - The end LSN of the transaction.
//
// Int64 - Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
func (p *parser) readCommit() (*Commit, error) {
	// Read flags, currently unused
	_, err := p.readUint8()
	if err != nil {
		return nil, err
	}

	lsn, err := p.readUint64()
	if err != nil {
		return nil, err
	}

	endLSN, err := p.readUint64()
	if err != nil {
		return nil, err
	}

	commitTimestamp, err := p.readTimestamp()
	if err != nil {
		return nil, err
	}

	return &Commit{
		LSN:        lsn,
		EndLSN:     endLSN,
		CommitTime: commitTimestamp,
	}, nil
}

// readType parses a TYPE according to the following specs:
//
// Byte1('Y') - Identifies the message as a type message.
//
// Int32 - ID of the data type.
//
// String - Namespace (empty string for pg_catalog).
//
// String - Name of the data type.
func (p *parser) readType() (*Typ, error) {
	dataTypeID, err := p.readUint32()
	if err != nil {
		return nil, err
	}

	namespace, err := p.readString()
	if err != nil {
		return nil, err
	}

	name, err := p.readString()
	if err != nil {
		return nil, err
	}

	return &Typ{
		DataTypeID: dataTypeID,
		Namespace:  namespace,
		Name:       name,
	}, nil
}

// readTruncate parses a TRUNCATE according to the following specs:
//
// Byte1('T') - Identifies the message as a truncate message.
//
// Int32 - Number of relations
//
// Int8 - Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY
//
// Int32 - ID of the relation corresponding to the ID in the relation message. This field is repeated for each relation.
func (p *parser) readTruncate() (*Truncate, error) {
	numRelations, err := p.readUint32()
	if err != nil {
		return nil, err
	}
	options, err := p.readUint8()
	if err != nil {
		return nil, err
	}

	var relationIDs []uint32
	for i := 0; i < int(numRelations); i++ {
		id, err := p.readUint32()
		if err != nil {
			return nil, err
		}
		relationIDs = append(relationIDs, id)
	}

	return &Truncate{
		NumRelations: numRelations,
		Option:       TruncateOption(options),
		ID:           relationIDs,
	}, nil
}

// readTupleData parses tuple data, according to the following specs:
//
// TupleData
//
// Int16 - Number of columns. Next, one of the following submessages appears for each column (except generated columns):
//
// Byte1('n') - Identifies the data as NULL value.
//
// Or
//
// Byte1('u') - Identifies unchanged TOASTed value (the actual value is not sent).
//
// Or
//
// Byte1('t') - Identifies the data as text formatted value.
//
// Int32 - Length of the column value.
//
// Byten - The value of the column, in text format. (A future release might support additional formats.) n is the above length.
func (p *parser) readTupleData() ([]*Tuple, error) {
	numCols, err := p.readUint16()
	if err != nil {
		return nil, err
	}

	var tupleData []*Tuple
	for c := 0; c < int(numCols); c++ {
		id, err := p.readNext()
		if err != nil {
			return nil, err
		}

		tpl := &Tuple{
			ValueType: TupleValueType(id),
		}

		if id == NullTupleValueType {
			tpl.Value = &NullTupleDataValue{}
			tupleData = append(tupleData, tpl)
			continue
		}

		lenCol, err := p.readUint32()
		if err != nil {
			return nil, err
		}

		if lenCol > 0 {
			data, err := p.readBytes(int(lenCol))
			if err != nil {
				return nil, err
			}

			if id == TextFormattedTupleValueType {
				tpl.Value = &TextFormattedTupleDataValue{
					Data: string(data),
				}
			} else {
				tpl.Value = &RawTupleDataValue{
					Data: data,
				}
			}
		}

		tupleData = append(tupleData, tpl)
	}

	return tupleData, nil
}

// readTimestamp parses a timestamp which is in number of microseconds since PostgreSQL epoch (2000-01-01).
func (p *parser) readTimestamp() (time.Time, error) {
	microsecondsSincePGEpoch, err := p.readUint64()
	if err != nil {
		return time.Time{}, err
	}
	return postgresEpoch.Add(time.Duration(microsecondsSincePGEpoch) * time.Microsecond), nil
}

func (p *parser) readUint64() (uint64, error) {
	buf, err := p.readBytes(8)
	if err != nil {
		return 0, err
	}
	return order.Uint64(buf), nil
}

func (p *parser) readUint32() (uint32, error) {
	buf, err := p.readBytes(4)
	if err != nil {
		return 0, err
	}
	return order.Uint32(buf), nil
}

func (p *parser) readUint16() (uint16, error) {
	buf, err := p.readBytes(2)
	if err != nil {
		return 0, err
	}
	return order.Uint16(buf), nil
}

func (p *parser) readUint8() (uint8, error) {
	ch, err := p.readNext()
	if err != nil {
		return 0, err
	}
	return ch, nil
}

func (p *parser) readBytes(num int) ([]byte, error) {
	return p.msg.Next(num), nil
}

func (p *parser) readString() (string, error) {
	// Read until \0
	s, err := p.msg.ReadBytes(0)
	if err != nil {
		return "", err
	}
	return string(s[:len(s)-1]), nil
}

func (p *parser) readNext() (byte, error) {
	b, err := p.msg.ReadByte()
	if err != nil {
		return 0, err
	}
	p.ch = b
	return b, nil
}

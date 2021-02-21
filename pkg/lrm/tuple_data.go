package lrm

import (
	"encoding/hex"
	"fmt"
)

// TupleValueType enumerates the possible types of tuple values
type TupleValueType byte

const (
	// NullTupleValueType identifies the data as NULL value.
	NullTupleValueType = 'n'
	// UnchangedTupleValueType identifies unchanged TOASTed value (the actual value is not sent).
	UnchangedTupleValueType = 'u'
	// TextFormattedTupleValueType identifies the data as text formatted value.
	TextFormattedTupleValueType = 't'
)

func (t TupleValueType) String() string {
	switch t {
	case NullTupleValueType:
		return "Null"
	case UnchangedTupleValueType:
		return "Unchanged"
	case TextFormattedTupleValueType:
		return "Text Formatted"
	}
	return fmt.Sprintf("Unhandled Type: %d", t)
}

// Tuple contains the metadata identifying the type of tuple value and the value itself
type Tuple struct {
	// ValueType identifies the type of value
	ValueType TupleValueType
	// Value holds the value payload
	Value TupleDataValue
}

// TupleDataValue is a marker interface for the different types of tuple values
type TupleDataValue interface {
	isTupleData()
}

// RawTupleDataValue contains the raw value payload
type RawTupleDataValue struct {
	// Data raw value
	Data []byte
}

func (r *RawTupleDataValue) isTupleData() {

}

func (r *RawTupleDataValue) String() string {
	return hex.EncodeToString(r.Data)
}

// TextFormattedTupleDataValue contains the text formatted value of the data payload
type TextFormattedTupleDataValue struct {
	Data string
}

func (t *TextFormattedTupleDataValue) String() string {
	return t.Data
}

func (t *TextFormattedTupleDataValue) isTupleData() {
}

// NullTupleDataValue indicates the value was NULL
type NullTupleDataValue struct {
}

func (n *NullTupleDataValue) String() string {
	return "NULL"
}

func (n *NullTupleDataValue) isTupleData() {
}

var _ TupleDataValue = &NullTupleDataValue{}

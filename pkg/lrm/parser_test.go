package lrm_test

import (
	"encoding/hex"
	"github.com/csueiras/pglrmparser/pkg/lrm"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		msg     []byte
		want    lrm.LRM
		wantErr bool
	}{
		{
			name: "Begin",
			msg:  decode("4200000000017729d000025ece2a7358c600000275"),
			want: &lrm.Begin{
				LSN:        24586704,
				CommitTime: time.Unix(0, 1613874321897670000).UTC(),
				XID:        629,
			},
		},
		{
			name: "Commit",
			msg:  decode("430000000000017740b800000000017740e800025ece6fadeaf5"),
			want: &lrm.Commit{
				LSN:        24592568,
				EndLSN:     24592616,
				CommitTime: time.Unix(0, 1613875483364085000).UTC(),
			},
		},
		{
			name: "Insert: Two columns, Text Formatted",
			msg:  decode("49000040054e000274000000023230740000000c48656c6c6f20576f726c6421"),
			want: &lrm.Insert{
				RelationID: 16389,
				NewTuple:   true,
				TupleData: []*lrm.Tuple{
					{
						ValueType: lrm.TextFormattedTupleValueType,
						Value:     &lrm.TextFormattedTupleDataValue{Data: "20"},
					},
					{
						ValueType: lrm.TextFormattedTupleValueType,
						Value:     &lrm.TextFormattedTupleDataValue{Data: "Hello World!"},
					},
				},
			},
		},
		{
			name: "Insert: Null Column",
			msg:  decode("49000040054e0002740000000232316e"),
			want: &lrm.Insert{
				RelationID: 16389,
				NewTuple:   true,
				TupleData: []*lrm.Tuple{
					{
						ValueType: lrm.TextFormattedTupleValueType,
						Value:     &lrm.TextFormattedTupleDataValue{Data: "21"},
					},
					{
						ValueType: lrm.NullTupleValueType,
						Value:     &lrm.NullTupleDataValue{},
					},
				},
			},
		},
		{
			name: "Delete",
			msg:  decode("44000040054b00027400000001326e"),
			want: &lrm.Delete{
				RelationID: 16389,
				IsKey:      true,
				IsOld:      false,
				TupleData:  nil,
			},
		},
		{
			name: "Update",
			msg:  decode("55000040054e0002740000000135740000000dc2a1486f6c61204d756e646f21"),
			want: &lrm.Update{
				RelationID: 16389,
				IsKey:      false,
				IsOld:      false,
				TupleData:  nil,
			},
		},
		{
			name: "Relation",
			msg:  decode("52000040057075626c6963006c6f6773006400020169640000000014ffffffff006d73670000000019ffffffff"),
			want: &lrm.Relation{
				RelationID: 16389,
				Namespace:  "public",
				Name:       "logs",
				Replica:    100,
				Columns: []*lrm.Column{
					{
						IsKey:            true,
						Name:             "id",
						DataType:         20,
						DataTypeModifier: 4294967295,
					},
					{
						IsKey:            false,
						Name:             "msg",
						DataType:         25,
						DataTypeModifier: 4294967295,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := lrm.Parse(tt.msg)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func BenchmarkParse(b *testing.B) {
	benchmarks := []struct {
		Name    string
		Message []byte
	}{
		{
			Name:    "Insert",
			Message: decode("49000040054e000274000000023230740000000c48656c6c6f20576f726c6421"),
		},
		{
			Name:    "Update",
			Message: decode("55000040054e0002740000000135740000000dc2a1486f6c61204d756e646f21"),
		},
		{
			Name:    "Delete",
			Message: decode("44000040054b00027400000001326e"),
		},
		{
			Name:    "Relation",
			Message: decode("52000040057075626c6963006c6f6773006400020169640000000014ffffffff006d73670000000019ffffffff"),
		},
		{
			Name:    "Begin",
			Message: decode("4200000000017729d000025ece2a7358c600000275"),
		},
		{
			Name:    "Commit",
			Message: decode("430000000000017740b800000000017740e800025ece6fadeaf5"),
		},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.Name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = lrm.Parse(benchmark.Message)
			}
		})
	}
}

func decode(s string) []byte {
	buf, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return buf
}

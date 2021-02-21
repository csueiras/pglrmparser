# pglrmparser
![Tests](https://github.com/csueiras/pglrmparser/workflows/run%20tests/badge.svg?branch=develop)
[![Coverage Status](https://coveralls.io/repos/github/csueiras/pglrmparser/badge.svg?branch=develop)](https://coveralls.io/github/csueiras/pglrmparser?branch=develop)
[![Go Report Card](https://goreportcard.com/badge/github.com/csueiras/pglrmparser)](https://goreportcard.com/report/github.com/csueiras/pglrmparser)
[![GitHub tag (latest SemVer pre-release)](https://img.shields.io/github/v/tag/csueiras/pglrmparser?include_prereleases&sort=semver)](https://github.com/csueiras/pglrmparser/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

_pglrmparser_ is a library for parsing [Postgres Logical Replication Message format](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html). This tool in conjunction with something like [pglogrepl](https://github.com/jackc/pglogrepl/) can allow you to easily parse our WAL data from Postgres.

## Usage

```go
package main

import (
	"encoding/hex"
	"fmt"
	"github.com/csueiras/pglrmparser/pkg/lrm"
)

func main() {
	input := "49000040054e000274000000023230740000000c48656c6c6f20576f726c6421"
	buf, _ := hex.DecodeString(input)
	msg, err := lrm.Parse(buf)
	if err != nil {
		panic(err)
	}

	insert := msg.(*lrm.Insert)
	fmt.Println("Insert:")
	fmt.Printf("Relation ID: %d\n", insert.RelationID)
	for i, datum := range insert.TupleData {
		fmt.Printf("Column #%d\n", i+1)
		fmt.Printf("Data: %v\n", datum.Value)
		fmt.Println()
	}
}
```

Produces:
```text
Insert:
Relation ID: 16389
Column #1
Data: 20

Column #2
Data: Hello World!
```
**More documentation to come soon**
// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Copied from https://golang.org/src/encoding/csv/writer.go but modified to
// support the SQL CSV use case. If the field is type string, then the field
// is always quoted.

package main

import (
	"bufio"
	"fmt"
	"io"
	"reflect"

	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

// A Writer writes records to a CSV file encoding using SQL syntax.
//
// As returned by NewWriter, a Writer writes records terminated by a
// newline and uses ',' as the field delimiter. The exported fields can be
// changed to customize the details before the first call to Write or WriteAll.
//
type SqlCsvWriter struct {
	Comma   string // Field delimiter (defaults ",")
	Newline string // Line terminator (defaults "\n")
	w       *bufio.Writer
}

// SqlCsvWriter returns a new SqlCsvWriter that writes to w.
func NewSqlCsvWriter(w io.Writer) *SqlCsvWriter {
	return &SqlCsvWriter{
		Comma:   ",",
		Newline: "\n",
		w:       bufio.NewWriter(w),
	}
}

func writeValue(buf *bufio.Writer, node *sqlparser.SQLVal) (int, error) {
	switch node.Type {
	case sqlparser.StrVal:
		sqltypes.MakeTrusted(sqltypes.VarBinary, node.Val).EncodeSQL(buf)

		// EncodeSQL does not return errors :(
		return 0, nil

	case sqlparser.IntVal, sqlparser.FloatVal, sqlparser.HexNum:
		return fmt.Fprintf(buf, "%s", []byte(node.Val))
	case sqlparser.HexVal:
		return fmt.Fprintf(buf, "X'%s'", []byte(node.Val))
	case sqlparser.BitVal:
		return fmt.Fprintf(buf, "B'%s'", []byte(node.Val))
	case sqlparser.ValArg:
		return buf.WriteString(string(node.Val))
	}
	panic(fmt.Sprintf("unexpected SQL tyoe %q", reflect.TypeOf(node.Type)))
}

func (w *SqlCsvWriter) WriteHeader(header []*sqlparser.ColumnDefinition) error {
	for n, c := range header {
		if n > 0 {
			if _, err := w.w.WriteString(w.Comma); err != nil {
				return err
			}
		}

		if _, err := w.w.WriteString(c.Name.String()); err != nil {
			return err
		}
	}
	return nil
}

// Writer writes a single CSV record to w along with any necessary quoting.
// A record is a slice of sqlparser.Expr with each string being one field.
func (w *SqlCsvWriter) Write(record []sqlparser.Expr) error {
	for n, expr := range record {
		if n > 0 {
			if _, err := w.w.WriteString(w.Comma); err != nil {
				return err
			}
		}

		var err error
		switch expr := expr.(type) {
		case *sqlparser.SQLVal:
			_, err = writeValue(w.w, expr)

		case *sqlparser.NullVal:
			_, err = w.w.WriteString("NULL")

		default:
			return fmt.Errorf("unsupported complex expression %q", reflect.TypeOf(expr))
		}

		if err != nil {
			return err
		}
	}

	_, err := w.w.WriteString(w.Newline)
	return err
}

// Flush writes any buffered data to the underlying io.Writer.
// To check if an error occurred during the Flush, call Error.
func (w *SqlCsvWriter) Flush() {
	w.w.Flush()
}

// Error reports any error that has occurred during a previous Write or Flush.
func (w *SqlCsvWriter) Error() error {
	_, err := w.w.Write(nil)
	return err
}

// WriteAll writes multiple CSV records to w using Write and then calls Flush.
func (w *SqlCsvWriter) WriteAll(records [][]sqlparser.Expr) error {
	for _, record := range records {
		if err := w.Write(record); err != nil {
			return err
		}
	}
	return w.w.Flush()
}

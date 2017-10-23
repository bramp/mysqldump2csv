// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"bufio"
	"fmt"
	"io"
	"reflect"

	"github.com/xwb1989/sqlparser"
)

// A SQLCsvWriter writes records to a CSV file encoding using SQL syntax. That is
// strings are always quoted, and int/hex/binary are encoded in their appropriate
// form.
type SQLCsvWriter struct {
	Comma   string // Field delimiter (defaults ",")
	Newline string // Line terminator (defaults "\n")
	w       *bufio.Writer
}

// NewSQLCsvWriter returns a new SqlCsvWriter that writes to w.
func NewSQLCsvWriter(w io.Writer) *SQLCsvWriter {
	return &SQLCsvWriter{
		Comma:   ",",
		Newline: "\n",
		w:       bufio.NewWriter(w),
	}
}

// WriteHeader writes a header row to the csv.
func (w *SQLCsvWriter) WriteHeader(header []*sqlparser.ColumnDefinition) error {
	for i, c := range header {
		if _, err := w.w.WriteString(c.Name.String()); err != nil {
			return err
		}
		if i < len(header)-1 {
			if _, err := w.w.WriteString(w.Comma); err != nil {
				return err
			}
		}
	}
	_, err := w.w.WriteString(w.Newline)
	return err
}

// Writer writes a single CSV record to w along with any necessary quoting.
// A record is a slice of sqlparser.Expr with each string being one field.
func (w *SQLCsvWriter) Write(record []sqlparser.Expr) error {
	for i, expr := range record {
		var err error
		switch expr := expr.(type) {
		case *sqlparser.SQLVal, *sqlparser.NullVal:
			if _, err = w.w.WriteString(sqlparser.String(expr)); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported complex expression %q", reflect.TypeOf(expr))
		}

		if i < len(record)-1 {
			if _, err := w.w.WriteString(w.Comma); err != nil {
				return err
			}
		}
	}

	_, err := w.w.WriteString(w.Newline)
	return err
}

// Flush writes any buffered data to the underlying io.Writer.
func (w *SQLCsvWriter) Flush() error {
	return w.w.Flush()
}

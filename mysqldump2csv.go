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
//
// mysqldump2csv takes a MySQL dump, and extracts the fields echoing them as a CSV.
// by Andrew Brampton (bramp.net)
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	gzip "github.com/klauspost/pgzip" // (faster than "compress/gzip")
	"github.com/xwb1989/sqlparser"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

var (
	verbose = flag.Bool("verbose", false, "verbose output")

	delimiter   = flag.String("delimiter", ",", "field delimiter")
	newline     = flag.String("newline", "\n", "line terminator")
	tableFilter = flag.String("table", "", "filter the results to only this table")
	header      = flag.Bool("header", true, "print the CSV header")
	multi       = flag.Bool("multi", false, "a csv file is created for each table")
)

// Table holds information about a single Table, and keeps track of writing the output
// for it.
type Table struct {
	name    string
	columns []*sqlparser.ColumnDefinition
	out     io.Writer
	csv     *SQLCsvWriter
	count   int
}

type mySQLDump2Csv struct {
	tables map[string]*Table

	// Options
	delimiter   string
	newline     string
	header      bool
	tableFilter string

	// For multi output
	multi bool
	root  string

	// For single output
	out io.Writer // Default out
}

func newMySQLDump2Csv() *mySQLDump2Csv {
	return &mySQLDump2Csv{
		tables:    make(map[string]*Table),
		delimiter: ",",
		newline:   "\n",
		header:    true,
		out:       os.Stdout,
	}
}

// Close closes the file backing this table.
func (t *Table) Close() error {
	if t.csv == nil {
		return nil
	}

	if err := t.csv.Flush(); err != nil {
		return err
	}

	if out, ok := t.out.(io.Closer); ok {
		if err := out.Close(); err != nil {
			return err
		}
	}
	t.csv = nil
	t.out = nil

	return nil
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: mysqldump2csv [flags] <dump.sql>\n")
	flag.PrintDefaults()
}

func parseArgs() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
}

func vlog(format string, v ...interface{}) {
	if *verbose {
		log.Printf(format, v)
	}
}

// tableName returns the full qualified table name.
func tableName(n sqlparser.TableName) string {
	if n.Qualifier.String() != "" {
		return n.Qualifier.String() + "." + n.Name.String()
	}
	return n.Name.String()
}

func (app *mySQLDump2Csv) writeRows(t *Table, rows sqlparser.Values) error {
	for _, row := range rows {
		if err := t.csv.Write(row); err != nil {
			return err
		}
	}
	t.count += len(rows)
	return nil
}

func (app *mySQLDump2Csv) create(s *sqlparser.DDL) error {
	var columns []*sqlparser.ColumnDefinition
	if s.TableSpec != nil {
		columns = s.TableSpec.Columns
	} else {
		vlog("Create DDL is missing a TableSpec %q", sqlparser.String(s))
	}

	name := tableName(s.NewName)
	app.tables[name] = &Table{
		name:    name,
		columns: columns,
	}

	return nil
}

func (app *mySQLDump2Csv) openCsv(t *Table) error {
	if app.multi {
		filename := filepath.Join(app.root, t.name) + ".csv" // TODO(bramp) Ensure t.name is safe for filenames
		log.Printf("Creating %q for table %q", filename, t.name)
		out, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("Failed to create csv file: %s", err)
		}
		t.out = out
	} else {
		t.out = app.out
	}

	t.csv = NewSQLCsvWriter(t.out)
	t.csv.Comma = app.delimiter
	t.csv.Newline = app.newline

	if app.header {
		if len(t.columns) > 0 {
			if err := t.csv.WriteHeader(t.columns); err != nil {
				return err
			}
		} else {
			// TODO If the INSERT's s.Columns is specified use that.
			log.Printf("Table %q columns are unknown so no header printed.", t.name)
		}
	}
	return nil
}

func (app *mySQLDump2Csv) insert(s *sqlparser.Insert) error {
	if len(s.Columns) > 0 {
		return errors.New("insert statement specifies the columns, that is not currently supported")
	}

	name := tableName(s.Table)
	if app.tableFilter != "" && app.tableFilter != name {
		// Ignore this insert
		return nil
	}

	// Create state for this table the first time we try and insert to it
	t, found := app.tables[name]
	if !found {
		if !app.multi && len(app.tables) >= 1 {
			var othername string
			for othername = range app.tables {
				break
			}
			return fmt.Errorf("found INSERT statements for multiple tables %q and %q. Either use --table or --multi", othername, t.name)
		}

		t = &Table{
			name: name,
		}
		app.tables[name] = t
	}

	// Open the csv on the first attempt to write to it
	if t.csv == nil {
		if err := app.openCsv(t); err != nil {
			return err
		}
	}

	if values, ok := s.Rows.(sqlparser.Values); ok {
		return app.writeRows(t, values)
	}

	return fmt.Errorf("Unsupported INSERT statement for table %q: %s", t.name, reflect.TypeOf(s.Rows))
}

// Process reads the supplied stream and outputs csv files.
func (app *mySQLDump2Csv) Process(in io.Reader) error {
	buf := bufio.NewReader(in)
	tokens := sqlparser.NewTokenizer(buf)
	tokens.AllowComments = true

	for {
		// Keep parsing from the stream, allowing us to read multiple statements.
		s, err := sqlparser.ParseNext(tokens)
		if err == io.EOF {
			break
		}
		if err != nil {
			// The sqlparser is unable to parse empty statements, for example `/*comment*/;`. This returns
			// a syntax error, so we just ignore all errors and kept going.
			// TODO(bramp) Patch the upstream parser to allow empty statements.
			vlog("Error parsing sql: %s", err.Error())
		}

		if s != nil {
			switch s := s.(type) {
			case *sqlparser.Insert:
				if err := app.insert(s); err != nil {
					return err
				}

			case *sqlparser.DDL:
				if s.Action == sqlparser.CreateStr {
					if err := app.create(s); err != nil {
						return err
					}
				} else {
					vlog("Ignoring %q", sqlparser.String(s))
				}
			default:
				vlog("Ignoring %q", sqlparser.String(s))
			}
		}
	}

	return nil
}

// Close closes any open csv files.
func (app *mySQLDump2Csv) Close() error {
	if len(app.tables) == 0 {
		log.Printf("Found no tables.")
		return nil
	}

	for _, t := range app.tables {
		if err := t.Close(); err != nil {
			return fmt.Errorf("Failed to close csv files: %s", err)
		}

		log.Printf("Wrote %d rows for table %q", t.count, t.name)
	}
	return nil
}

func main() {
	parseArgs()

	app := newMySQLDump2Csv()
	app.delimiter = *delimiter
	app.newline = *newline
	app.header = *header
	app.tableFilter = *tableFilter
	app.multi = *multi

	for _, input := range flag.Args() {
		var in io.Reader
		if input == "-" {
			in = os.Stdin
		} else {
			var err error

			in, err = os.Open(input)
			if err != nil {
				log.Fatal(err)
			}

			if strings.HasSuffix(input, ".gz") {
				if in, err = gzip.NewReader(in); err != nil {
					log.Fatal(err)
				}
			}
		}

		if err := app.Process(in); err != nil {
			log.Fatal(err)
		}

		if in, ok := in.(io.Closer); ok {
			in.Close()
		}
	}

	if err := app.Close(); err != nil {
		log.Fatal(err)
	}
}

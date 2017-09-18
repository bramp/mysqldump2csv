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
	"reflect"
	"strings"
)

var (
	delimiter = flag.String("delimiter", ",", "field delimiter")
	newline   = flag.String("newline", "\n", "line terminator")

	verbose     = flag.Bool("verbose", false, "verbose output")
	header      = flag.Bool("header", true, "print the CSV header")
	tableFilter = flag.String("table", "", "filter the results to only this table")
	multi       = flag.Bool("multi", false, "a csv file is created for each table")
	input       string
)

/*
// rowWriter is a simple interface for writing a row of results
type rowWriter interface {
	Write(record []string) error
}
*/

// eofReader wraps a Reader but keeps track of when EOF happens.
type eofReader struct {
	io.Reader
	Eof bool
}

func (r *eofReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		r.Eof = true
	}
	return n, err
}

type Table struct {
	name          string
	columns       []*sqlparser.ColumnDefinition
	out           io.Writer
	csv           *SqlCsvWriter
	printedHeader bool
	count         int
}

func (t *Table) Close() error {
	if t.csv == nil {
		return nil
	}

	t.csv.Flush()
	if err := t.csv.Error(); err != nil {
		return err
	}

	if out, ok := t.out.(io.Closer); ok {
		if err := out.Close(); err != nil {
			return err
		}
	}

	return nil
}

// MultiSQLTokenizer is a wrapper around sqlparser.Tokenizer but replaces ';' as a io.EOF,
// so that multiple SQL statements can be read from a single io.Reader.
type MultiSQLTokenizer struct {
	sqlparser.Tokenizer
}

func NewMultiSQLTokenizer(r io.Reader) *MultiSQLTokenizer {
	return &MultiSQLTokenizer{
		sqlparser.Tokenizer{InStream: bufio.NewReader(r)},
	}
}

func (tkn *MultiSQLTokenizer) Scan() (int, []byte) {
	t, v := tkn.Tokenizer.Scan()
	if t == ';' {
		return 0, nil
	}
	fmt.Printf("%d %q\n", t, v)
	return t, v
}

func tokenError(tokens *sqlparser.Tokenizer, expected string, token int, value []byte) error {
	// TODO Print out token type and line number
	if token == 0 {
		value = []byte("EOF")
	}
	return fmt.Errorf("pos: %d expected %s found %q", tokens.Position, expected, string(value))
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
	input = flag.Args()[0]
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

type mysqldump2csv struct {
	tables      map[string]*Table
	singleTable *Table
}

func NewMysqldump2csv() *mysqldump2csv {
	return &mysqldump2csv{
		tables: make(map[string]*Table),
	}
}

func (app *mysqldump2csv) writeRow(t *Table, row sqlparser.ValTuple) error {
	/*
		fields := make([]string, len(row))

		for i, expr := range row {
			switch expr := expr.(type) {
			case *sqlparser.SQLVal:
				// TODO Do something with expr.Type
				fields[i] = string(expr.Val)

			case *sqlparser.NullVal:
				fields[i] = "NULL"
			default:
				return fmt.Errorf("values contain unsupported complex expression %q", reflect.TypeOf(expr))
			}
		}

		return t.csv.Write(row)
	*/
	return t.csv.Write(row)
}

func (app *mysqldump2csv) writeRows(t *Table, rows sqlparser.Values) error {
	for _, row := range rows {
		if err := app.writeRow(t, row); err != nil {
			return err
		}
	}
	t.count += len(rows)
	return nil
}

func (app *mysqldump2csv) create(s *sqlparser.DDL) error {
	name := tableName(s.NewName)
	app.tables[name] = &Table{
		name:    name,
		columns: s.TableSpec.Columns,
	}

	return nil
}

func (app *mysqldump2csv) insert(s *sqlparser.Insert) error {
	if len(s.Columns) > 0 {
		return errors.New("Insert statement specifies the columns, that is not currently supported.")
	}

	name := tableName(s.Table)
	if *tableFilter != "" && *tableFilter != name {
		// Ignore this insert
		return nil
	}

	// Create state for this table the first time we try and insert to it
	t, found := app.tables[name]
	if !found {
		t = &Table{
			name: name,
		}
		app.tables[name] = t
	}

	if !*multi {
		if app.singleTable != nil && app.singleTable != t {
			return fmt.Errorf("Found INSERT statements for multiple tables %q and %q. Either use --table or --multi.", app.singleTable.name, t.name)
		}
		app.singleTable = t
	}

	// Open the csv on the first attempt to write to it
	if t.csv == nil {
		if *multi {
			filename := t.name + ".csv"
			log.Printf("Creating %q for table %q", filename, t.name)
			out, err := os.Create(filename)
			if err != nil {
				return fmt.Errorf("Failed to create csv file: %s", err)
			}
			t.out = out
		} else {
			// TODO Add support for writing a single table to a filename (instead of stdout)
			t.out = os.Stdout
		}

		t.csv = NewSqlCsvWriter(t.out)
		t.csv.Comma = *delimiter
		t.csv.Newline = *newline

		if *header && !t.printedHeader {
			t.printedHeader = true

			if len(t.columns) > 0 {
				if err := t.csv.WriteHeader(t.columns); err != nil {
					return err
				}
			} else {
				// TODO If the INSERT's s.Columns is specified use that.
				log.Printf("Table %q columns are unknown so no header printed.", t.name)
			}
		}
	}

	if values, ok := s.Rows.(sqlparser.Values); ok {
		return app.writeRows(t, values)
	}

	return fmt.Errorf("Unsupported INSERT statement for table %q: %s", t.name, reflect.TypeOf(s.Rows))
}

func (app *mysqldump2csv) Process(in io.Reader) error {
	// We have to do this hack with the eofReader, because the sqlparser does
	// not seem to handle the EOF correctly in all cases.
	r := &eofReader{Reader: in}
	buf := bufio.NewReader(r)

	for !r.Eof {
		tokens := &sqlparser.Tokenizer{
			InStream:      buf,
			AllowComments: true,
		}

		// Keep parsing from the stream, allowing us to read multiple statements.
		s, err := sqlparser.ParseFromTokenizer(tokens)
		if err != nil {
			// The sqlparser is unable to parse empty statements, for example `/*comment*/;`. This returns
			// a syntax error, so we just ignore all errors and kept going.
			// TODO Patch the upstream parser to allow empty statements.
			vlog("Error parsing sql: %s", err)
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
					vlog("Ignoring %q", s)
				}
			default:
				vlog("Ignoring %q", s)
			}
		}
	}

	return nil
}

func (app *mysqldump2csv) Close() error {
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

	app := NewMysqldump2csv()
	if err := app.Process(in); err != nil {
		log.Fatal(err)
	}
	if err := app.Close(); err != nil {
		log.Fatal(err)
	}
}

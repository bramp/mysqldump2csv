// mysqldump2csv takes a MySQL dump, and extracts the fields echoing them as a CSV.
// by Andrew Brampton (bramp.net)

package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	gzip "github.com/klauspost/pgzip" // (faster than "compress/gzip")
	"github.com/xwb1989/sqlparser"
	"io"
	"log"
	"os"
	"strings"
	"unicode/utf8"
)

var (
	comma     rune // The actual delimiter rune
	delimiter = flag.String("delimiter", ",", "field delimiter")
	//header    = flag.Bool("header", true, "print the CSV header")
)

// rowWriter is a simple interface for writing a row of results
type rowWriter interface {
	Write(record []string) error
}

func tokenError(tokens *sqlparser.Tokenizer, expected string, token int, value []byte) error {
	// TODO Print out token type and line number
	if token == 0 {
		value = []byte("EOF")
	}
	return fmt.Errorf("pos: %d expected %s found %q", tokens.Position, expected, string(value))
}

// consumeRow reads a row of VALUES from the Tokenizer. It is expected that the first token is
// the '(' at the beginning of the row, and the Tokenizer will read and consume the ending ')'.
func consumeRow(w rowWriter, tokens *sqlparser.Tokenizer) error {
	token, value := tokens.Scan()
	switch token {
	case 0:
		return io.EOF
	}

	if token != '(' {
		return tokenError(tokens, `'('`, token, value)
	}

	var err error
	var row []string

	// Read one full row
	for {
		token, value = tokens.Scan()
		if token == ')' {
			break
		} else if token == '-' {
			// The minus sign infront of the number, doesn't get treated as part of the number, so handle this manually.
			token, value = tokens.Scan()
			value = []byte("-" + string(value))
		} else if token == sqlparser.NULL {
			// TODO Handle this in some special way, so it doesn't look like empty string
			value = []byte("NULL")
		}

		row = append(row, string(value))

		token, value = tokens.Scan()
		if token != ',' {
			break
		}
	}

	if token != ')' {
		return tokenError(tokens, `')'`, token, value)
	}

	// Write out the CSV
	if err := w.Write(row); err != nil {
		return err
	}

	return err
}

// scanUntilValues scans and discards tokens until the "VALUES" keyword is found.
// Returning the table name, and any error
func scanUntilValues(tokens *sqlparser.Tokenizer) (string, error) {
	var prevToken int
	var prevValue []byte

	for count := 0; count < 10000; count++ {
		token, value := tokens.Scan()
		switch token {
		case 0:
			return "", io.EOF

		case sqlparser.VALUES:
			if prevToken != sqlparser.ID {
				return "", errors.New("found VALUES but it was not preceeded by a table name")
			}
			// prevValue should be the table name
			return string(prevValue), nil
		}
		prevToken = token
		prevValue = value
	}

	// Give up!
	return "", errors.New("unable to find 'INSERT INTO ... VALUES'")
}

func process(w rowWriter, tokens *sqlparser.Tokenizer) error {
	count := 0
	currentTable := ""

NextTable:
	for {
		table, err := scanUntilValues(tokens)
		if err == io.EOF {
			// Allow EOF between tables
			if currentTable == "" {
				return fmt.Errorf("found no tables")
			}

			log.Printf("Read %d rows for %q table", count, currentTable)
			return nil

		} else if err != nil {
			return err
		}

		if table != currentTable {
			if currentTable != "" {
				log.Printf("Read %d rows for %q table", count, currentTable)
				count = 0
			}
			// Sometimes there are multiple INSERT statements for the same table
			currentTable = table

			log.Printf("Processing %q table", table)
		}

		for {
			// Consume a single row
			if err := consumeRow(w, tokens); err != nil {
				return err
			}

			count++

			token, value := tokens.Scan()
			switch token {
			case ',':
				// Comma between sets of VALUES.
				continue
			case ';':
				// Scan to see if there is another table
				continue NextTable
			default:
				return tokenError(tokens, `',' or ';'`, token, value)
			}
		}
	}

	panic("Shouldn't get here")
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

	comma, _ = utf8.DecodeRuneInString(*delimiter)
	if string(comma) != *delimiter {
		fmt.Fprintf(os.Stderr, "invalid delimiter must be a single character\n")
		os.Exit(2)
	}
}

func main() {
	parseArgs()

	filename := os.Args[1]

	var in io.Reader

	if filename == "-" {
		in = os.Stdin
	} else {
		var err error

		in, err = os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}

		if strings.HasSuffix(filename, ".gz") {
			if in, err = gzip.NewReader(in); err != nil {
				log.Fatal(err)
			}
		}
	}

	tokens := &sqlparser.Tokenizer{InStream: bufio.NewReader(in)}
	w := csv.NewWriter(os.Stdout)
	w.Comma = comma

	if err := process(w, tokens); err != nil {
		log.Fatal(err)
	}

	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatal(err)
	}
}

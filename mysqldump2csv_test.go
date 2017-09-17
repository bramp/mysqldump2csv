package main

import (
	"github.com/kylelemons/godebug/pretty"
	"github.com/xwb1989/sqlparser"
	"io"
	"strings"
	"testing"
)

type FakeRowWriter struct {
	record []string
}

func (w *FakeRowWriter) Write(record []string) error {
	w.record = record
	return nil
}

func TestConsumeRow(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"()", []string{}},
		{"(1,2.0,-3,'string',NULL)", []string{"1", "2.0", "-3", "string", "NULL"}},
	}

	for _, test := range tests {
		in := strings.NewReader(test.input)
		out := &FakeRowWriter{}

		if err := consumeRow(out, &sqlparser.Tokenizer{InStream: in}); err != io.EOF {
			t.Errorf("consumeRow(..., %q) err = %q, want io.EOF", test.input, err)
		}
		if diff := pretty.Compare(out.record, test.want); diff != "" {
			t.Errorf("consumeRow(..., %q) diff: (-got +want)\n%s", test.input, diff)
		}
	}
}

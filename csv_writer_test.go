package main

import (
	"bytes"
	"testing"

	"github.com/kylelemons/godebug/pretty"
	"github.com/xwb1989/sqlparser"
)

func TestNewSQLCsvWriter(t *testing.T) {
	header := []*sqlparser.ColumnDefinition{
		{Name: sqlparser.NewColIdent(string("string"))},
		{Name: sqlparser.NewColIdent(string("int"))},
		{Name: sqlparser.NewColIdent(string("float"))},
		{Name: sqlparser.NewColIdent(string("hex"))},
		{Name: sqlparser.NewColIdent(string("bit"))},
		{Name: sqlparser.NewColIdent(string("null"))},
	}
	row := []sqlparser.Expr{
		sqlparser.NewStrVal([]byte("a")),
		sqlparser.NewIntVal([]byte("1")),
		sqlparser.NewFloatVal([]byte("2.3")),
		sqlparser.NewHexVal([]byte("4567")),
		sqlparser.NewBitVal([]byte("0110")),
		&sqlparser.NullVal{},
	}

	var b bytes.Buffer
	w := NewSQLCsvWriter(&b)

	if err := w.WriteHeader(header); err != nil {
		t.Errorf("WriteHeader(...) err %s, want nil", err)
	}
	if err := w.Write(row); err != nil {
		t.Errorf("Write(...) err %s, want nil", err)
	}
	if err := w.Flush(); err != nil {
		t.Errorf("Flush() err %s, want nil", err)
	}

	got := b.String()
	want := "string,int,float,hex,bit,null\n" +
		"'a',1,2.3,X'4567',B'0110',null\n"

	if diff := pretty.Compare(got, want); diff != "" {
		t.Errorf("diff: (-got +want)\n%s", diff)
	}
}

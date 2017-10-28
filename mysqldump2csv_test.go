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
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestMain(m *testing.M) {
	*verbose = true
	os.Exit(m.Run())
}

func processFile(t *testing.T, app *mySQLDump2Csv, input string) {
	f, err := os.Open(input)
	if err != nil {
		t.Fatalf("Failed to open input testdata: %s", err)
	}

	if err := app.Process(f); err != nil {
		t.Errorf("[%q] app.Process(...) err = %s, want nil", input, err)
	}

	if err := app.Close(); err != nil {
		t.Errorf("[%q] app.Close() err = %s, want nil", input, err)
	}
}

// compare is a quick helper function, to compare the two outputs
// allowing for any trailing newlines to be removed.
func compare(got, want []byte) string {
	g := strings.TrimSuffix(string(got), "\n")
	w := strings.TrimSuffix(string(want), "\n")

	return pretty.Compare(g, w)
}

func TestMySQLDump2CsvSingle(t *testing.T) {
	var b bytes.Buffer

	app := newMySQLDump2Csv()
	app.out = &b

	want, err := ioutil.ReadFile("testdata/single.csv")
	if err != nil {
		t.Fatalf("Failed to open output testdata: %s", err)
	}

	filename := "testdata/single.sql"
	processFile(t, app, filename)

	// Checkout stdout
	got := b.Bytes()
	if diff := compare(got, want); diff != "" {
		t.Errorf("[%q] app.Process(...) (-got +want)\n%s", filename, diff)
	}
}

func TestMySQLDump2CsvMulti(t *testing.T) {
	root, err := ioutil.TempDir("", "mysqldump2csv")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %s", err)
	}

	defer os.RemoveAll(root)

	app := newMySQLDump2Csv()
	app.multi = true
	app.root = root

	filename := "testdata/multi.sql"
	processFile(t, app, filename)

	// Test for the two output files
	for _, file := range []string{"one.csv", "two.csv"} {
		want, err := ioutil.ReadFile(filepath.Join("testdata", file))
		if err != nil {
			t.Fatalf("Failed to open output testdata: %s", err)
		}

		got, err := ioutil.ReadFile(filepath.Join(root, file))
		if err != nil {
			t.Errorf("Failed to open output: %s", err)
		}

		if diff := compare(got, want); diff != "" {
			t.Errorf("[%q] app.Process(...) %s (-got +want)\n%s", filename, file, diff)
		}
	}
}

func TestMySQLDump2CsvNotSupported(t *testing.T) {
	var b bytes.Buffer

	app := newMySQLDump2Csv()
	app.out = &b

	input := "testdata/not_supported.sql"

	f, err := os.Open(input)
	if err != nil {
		t.Fatalf("Failed to open input testdata: %s", err)
	}

	if err := app.Process(f); err == nil {
		t.Errorf("[%q] app.Process(...) err = nil, want 'not currently supported'", input, err)
	}

}

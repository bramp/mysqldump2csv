mysqldump2csv
=============
by Andrew Brampton ([bramp.net](https://bramp.net)) (c) 2017

Convert MySQL SQL Dumps to CSV. Originally developed to convert [Wikipedia database backups](https://dumps.wikimedia.org/backup-index.html) into something more parsable. Uses a SQL parser to correctly parse the file, instead of hacky regexes (or likewise) that are unreliable.

<!--
![Go](https://img.shields.io/badge/Go-1.9+-brightgreen.svg)
[![Build Status](https://img.shields.io/travis/bramp/hilbert.svg)](https://travis-ci.org/google/hilbert)
[![Coverage](https://img.shields.io/coveralls/google/hilbert.svg)](https://coveralls.io/github/google/hilbert)
[![Report card](https://goreportcard.com/badge/github.com/google/hilbert)](https://goreportcard.com/report/github.com/google/hilbert)
[![GoDoc](https://godoc.org/github.com/google/hilbert?status.svg)](https://godoc.org/github.com/google/hilbert)
[![Libraries.io](https://img.shields.io/librariesio/github/google/hilbert.svg)](https://libraries.io/github/google/hilbert)
-->

[GitHub](https://github.com/bramp/mysqldump2csv)

Install
-------

```bash
go get -u bramp.net/mysqldump2csv
```

Usage
-----

```man
mysqldump2csv - Convert MySQL SQL Dumps to CSV

Usage:
  mysqldump2csv [flags] <dump.sql>

Flags:
  -delimiter string		    field delimiter (default ",")
```

Example:

```bash
$ mysqldump2csv enwiki-20170901-page.sql.gz > enwiki-20170901-page.csv
```

Related
-------

After writing this I found a almost identical application, [mysqldump-to-csv](https://github.com/jamesmishra/mysqldump-to-csv) written with the same goals.

TODO
----
[ ] Convince https://github.com/xwb1989/sqlparser/issues/11 to update to the latest parser supplied by youtube/vitess. This will fix a bug parsing the `CREATE TABLE` statement.
[ ] Parse the `CREATE TABLE` line, and output a header line with the column names.
[ ] Add better support for dumps with multiple tables.
[ ] Change the output CSV to always quote strings, and never quote numbers and NULL. 
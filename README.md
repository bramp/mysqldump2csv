# mysqldump2csv
by Andrew Brampton ([bramp.net](https://bramp.net))

Convert a MySQL SQL dumps to a CSV file. Originally developed to convert [Wikipedia database backups](https://dumps.wikimedia.org/backup-index.html) into something more parsable. Uses a SQL parser to correctly parse the file, instead of hacky regexes (or likewise) that are unreliable.

# Install

```bash
go install bramp.net/mysqldump2csv
```

# Usage

```man
mysqldump2csv - Convert MySQL SQL Dumps to CSV

Usage:
  mysqldump2csv [flags] <dump.sql>

Flags:
  -delimiter string		    field delimiter (default: ",")
  -newline string           line terminator (default: "\n")

  -header                   Print the CSV header (default: true)
  -multi                    A CSV file is created for each table (default: false)
  -table string             Filter the input to only this table (default: "")
  -verbose                  Verbose output (default: false)

```

Example:

```bash
$ mysqldump2csv enwiki-20170901-page.sql.gz > enwiki-20170901-page.csv
```

# Related

After writing this I found an almost identical application, [mysqldump-to-csv](https://github.com/jamesmishra/mysqldump-to-csv) written with the same goals.

# Licence (Apache 2)

*This is not an official Google product (experimental or otherwise), it is just
code that happens to be owned by Google.*

```
Copyright 2017 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
---
layout: global
title: Spark SQL CLI
displayTitle: Spark SQL CLI
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* Table of contents
{:toc}


The Spark SQL CLI is a convenient interactive command tool to run the Hive metastore service and execute SQL
queries input from the command line. Note that the Spark SQL CLI cannot talk to the Thrift JDBC server.

To start the Spark SQL CLI, run the following in the Spark directory:

    ./bin/spark-sql

Configuration of Hive is done by placing your `hive-site.xml`, `core-site.xml` and `hdfs-site.xml` files in `conf/`.

## Spark SQL Command Line Options

You may run `./bin/spark-sql --help` for a complete list of all available options.

    CLI options:
     -d,--define <key=value>          Variable substitution to apply to Hive
                                      commands. e.g. -d A=B or --define A=B
        --database <databasename>     Specify the database to use
     -e <quoted-query-string>         SQL from command line
     -f <filename>                    SQL from files
     -H,--help                        Print help information
        --hiveconf <property=value>   Use value for given property
        --hivevar <key=value>         Variable substitution to apply to Hive
                                      commands. e.g. --hivevar A=B
     -i <filename>                    Initialization SQL file
     -S,--silent                      Silent mode in interactive shell
     -v,--verbose                     Verbose mode (echo executed SQL to the
                                      console)

## The hiverc File

When invoked without the `-i`, the Spark SQL CLI will attempt to load `$HIVE_HOME/bin/.hiverc` and `$HOME/.hiverc` as initialization files.

## Path interpretation

Spark SQL CLI supports running SQL from initialization script file(`-i`) or normal SQL file(`-f`), If path url don't have a scheme component, the path will be handled as local file.
For example: `/path/to/spark-sql-cli.sql` equals to `file:///path/to/spark-sql-cli.sql`. User also can use Hadoop supported filesystems such as `s3://<mys3bucket>/path/to/spark-sql-cli.sql` or `hdfs://<namenode>:<port>/path/to/spark-sql-cli.sql`.

## Supported comment types

<table class="table">
<tr><th>Comment</th><th>Example</th></tr>
<tr>
  <td>simple comment</td>
  <td>
  <code>
      -- This is a simple comment.
      <br>
      SELECT 1;
  </code>
  </td>
</tr>
<tr>
  <td>bracketed comment</td>
  <td>
    <code>
        /* This is a bracketed comment. */
        <br>
        SELECT 1;
    </code>
  </td>
</tr>
<tr>
  <td>nested bracketed comment</td>
  <td>
    <code>
        /*  This is a /* nested bracketed comment*/ .*/
        <br>
        SELECT 1;
    </code>
  </td>
</tr>
</table>

## Spark SQL CLI Interactive Shell Commands

When `./bin/spark-sql` is run without either the `-e` or `-f` option, it enters interactive shell mode.
Use `;` (semicolon) to terminate commands. Notice:
1. The CLI use `;` to terminate commands only when it's at the end of line, and it's not escaped by `\\;`.
2. `;` is the only way to terminate commands. If the user types `SELECT 1` and presses enter, the console will just wait for input.
3. If the user types multiple commands in one line like `SELECT 1; SELECT 2;`, the commands `SELECT 1` and `SELECT 2` will be executed separately.
4. If `;` appears within a SQL statement (not the end of the line), then it has no special meanings:
   ```sql
   -- This is a ; comment
   SELECT ';' as a;
   ```
   This is just a comment line followed by a SQL query which returns a string literal.
   ```sql
   /* This is a comment contains ;
   */ SELECT 1;
   ```
   However, if ';' is the end of the line, it terminates the SQL statement. The example above will be terminated into  `/* This is a comment contains ` and `*/ SELECT 1`, Spark will submit these two commands separated and throw parser error (`unclosed bracketed comment` and `Syntax error at or near '*/'`).

<table class="table">
<tr><th>Command</th><th>Description</th></tr>
<tr>
  <td><code>quit</code> or <code>exit</code></td>
  <td>Exits the interactive shell.</td>
</tr>
<tr>
  <td><code>!&lt;command&gt;</code></td>
  <td>Executes a shell command from the Spark SQL CLI shell.</td>
</tr>
<tr>
  <td><code>dfs &lt;HDFS dfs command&gt;</code></td>
  <td>Executes a HDFS <a href="https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html#dfs">dfs command</a> from the Spark SQL CLI shell.</td>
</tr>
<tr>
  <td><code>&lt;query string&gt;</code></td>
  <td>Executes a Spark SQL query and prints results to standard output.</td>
</tr>
<tr>
  <td><code>source &lt;filepath&gt;</code></td>
  <td>Executes a script file inside the CLI.</td>
</tr>
</table>

## Examples

Example of running a query from the command line:

    ./bin/spark-sql -e 'SELECT COL FROM TBL'

Example of setting Hive configuration variables:

    ./bin/spark-sql -e 'SELECT COL FROM TBL' --hiveconf hive.exec.scratchdir=/home/my/hive_scratch
    
Example of setting Hive configuration variables and using it in the SQL query:

    ./bin/spark-sql -e 'SELECT ${hiveconf:aaa}' --hiveconf aaa=bbb --hiveconf hive.exec.scratchdir=/home/my/hive_scratch
    spark-sql> SELECT ${aaa};
    bbb
    
Example of setting Hive variables substitution:
    
    ./bin/spark-sql --hivevar aaa=bbb --define ccc=ddd
    spark-sql> SELECT ${aaa}, ${ccc};
    bbb ddd

Example of dumping data out from a query into a file using silent mode:

    ./bin/spark-sql -S -e 'SELECT COL FROM TBL' > result.txt

Example of running a script non-interactively:

    ./bin/spark-sql -f /path/to/spark-sql-script.sql

Example of running an initialization script before entering interactive mode:

    ./bin/spark-sql -i /path/to/spark-sql-init.sql

Example of entering interactive mode:

    ./bin/spark-sql
    spark-sql> SELECT 1;
    1
    spark-sql> -- This is a simple comment.
    spark-sql> SELECT 1;
    1

Example of entering interactive mode with escape `;` in comment:

    ./bin/spark-sql
    spark-sql>/* This is a comment contains \\;
             > It won't be terminated by \\; */
             > SELECT 1;
    1

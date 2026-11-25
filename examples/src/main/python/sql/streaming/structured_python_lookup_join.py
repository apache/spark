#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A Python data source that serves lookup joins from SQLite: a walk through the lookup contract.

DEMO ONLY. SQLite is a file, not a service. The driver ships a read-only copy of the file to
every executor, and each executor reads its own copy, so changes to the source file after the
query starts are not seen. A real lookup connector calls a network service, such as Postgres,
Redis or an HTTP API, from lookup().

The example joins a stream of events (user_id, segment, day) with the offers stored in SQLite:

    events LEFT JOIN offers
      ON offers.user_id = events.user_id
     AND offers.segment <=> events.segment
     AND offers.valid_from <= events.day AND events.day < offers.valid_until
     AND offers.country = 'KR'

Key concepts demonstrated:
- Hint mode: a source with reader() and lookupReader() is read by lookups only where the query
  adds the EXTERNAL_LOOKUP hint. Without reader(), the source is lookup-only.
- pushFilters() serving country = 'KR' in SQL.
- selectLookupPredicates() choosing =, <=> and a range, and leaving other candidates to Spark.
- maxParameterRowsPerRequest() fitting SQLite's limit on "?" markers.
- One SQL statement per request: the parameter rows become a VALUES table.
- Results as (ordinal, row) pairs, or as PyArrow RecordBatches with the ordinal in column 0.
- Shipping the file with addFile() (spark.addArtifacts(path, file=True) in Spark Connect) and
  reading it with SparkFiles.get() on the executor; a lazy connection and close().

Usage:
    bin/spark-submit examples/src/main/python/sql/streaming/structured_python_lookup_join.py

Expected output:
    +-------+-------+---+----------+
    |user_id|segment|day|     offer|
    +-------+-------+---+----------+
    |      1|   gold|  2|    gold-1|   valid_from 5 > day 2: only one match
    |      1|   gold|  6|    gold-1|   several matches for one parameter row
    |      1|   gold|  6|    gold-5|
    |      2|   NULL|  6|no-segment|   NULL segment matched through <=>
    |      3|   gold|  6|      NULL|   miss: the US offer fails country = 'KR'
    |      4|   gold|  6|      NULL|   miss: valid only from day 9
    |      5|   gold|  6|      NULL|   miss: unknown user
    +-------+-------+---+----------+
"""

import json
import os
import sqlite3
import tempfile

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.datasource import (
    DataSource,
    DataSourceLookupReader,
    DataSourceReader,
    EqualNullSafe,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    InputPartition,
    LessThan,
    LessThanOrEqual,
)

COLUMNS = ["user_id", "segment", "valid_from", "valid_until", "country", "offer"]
# IS is SQLite's null-safe "=", the <=> of Spark.
SQL_OPS = {
    EqualTo: "=",
    EqualNullSafe: "IS",
    LessThan: "<",
    LessThanOrEqual: "<=",
    GreaterThan: ">",
    GreaterThanOrEqual: ">=",
}
SQLITE_MAX_MARKERS = 999  # SQLite's limit on "?" markers in one statement before version 3.32


def connect(file_name):
    # The executor's local copy of the file, read-only. immutable=1: the copy never changes.
    path = SparkFiles.get(file_name)
    return sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)


class OffersDataSource(DataSource):
    """
    reader() + lookupReader() is hint mode: Spark reads the source by batch reads, and by
    lookups only in a join where the query adds the EXTERNAL_LOOKUP hint. Delete reader() to
    make the source lookup-only: then every stream-static join with it is a lookup join, with
    no hint, and a batch read of it fails.
    """

    @classmethod
    def name(cls):
        return "offers"

    def schema(self):
        return (
            "user_id BIGINT, segment STRING, valid_from BIGINT, valid_until BIGINT, "
            "country STRING, offer STRING"
        )

    def reader(self, schema):
        return OffersReader(self.options["file"])  # only the file name, never a path

    def lookupReader(self, schema):
        return OffersLookupReader(
            self.options["file"],
            select_range=self.options.get("select_range", "true") == "true",
            arrow=self.options.get("arrow", "false") == "true",
        )


class OffersReader(DataSourceReader):
    """The batch reader of hint mode: the whole table."""

    def __init__(self, file_name):
        self.file_name = file_name

    def read(self, partition: InputPartition):
        conn = connect(self.file_name)
        try:
            yield from conn.execute(f"SELECT {', '.join(COLUMNS)} FROM offers")
        finally:
            conn.close()


class OffersLookupReader(DataSourceLookupReader):
    # The planning methods run on the driver and may set state on self. Spark then pickles
    # self, and calls lookup() and close() on one copy per input partition, on the executors.

    def __init__(self, file_name, select_range, arrow):
        self.file_name = file_name
        self.select_range = select_range
        self.arrow = arrow
        self.country = None  # set by pushFilters
        self.on = []  # (column, SQL operator) per selected predicate, in order
        self.conn = None  # opened lazily on the executor; never pickled

    def pushFilters(self, filters):
        # Serve country = '<literal>' in SQL, and return the other filters. Spark checks every
        # filter again on the returned rows anyway, so serving one only lets us omit rows.
        for f in filters:
            is_country = isinstance(f, EqualTo) and f.attribute == ("country",)
            if is_country and isinstance(f.value, str):
                self.country = f.value
            else:
                yield f

    def selectLookupPredicates(self, candidates):
        # Each candidate compares a column with a LookupParameter, a value that Spark binds for
        # each lookup. Return the positions of the chosen candidates, in the order of the
        # fields of every parameter row: (user_id, segment, valid_from bound).
        def find(cls, column):
            return next(
                (
                    i
                    for i, c in enumerate(candidates)
                    if type(c) is cls and c.attribute == (column,)
                ),
                None,
            )

        chosen = [
            find(EqualTo, "user_id"),  # offers.user_id = events.user_id
            find(EqualNullSafe, "segment"),  # offers.segment <=> events.segment (nullable)
        ]
        if self.select_range:
            # offers.valid_from <= events.day. A range whose value reads a stream column makes
            # each request hold ONE parameter row, and turns Spark's lookup cache off for the
            # join. With select_range=false, Spark checks the range itself, and a request holds
            # many parameter rows.
            chosen.append(find(LessThanOrEqual, "valid_from"))
        # Left unselected on purpose: valid_until > ? (from events.day < offers.valid_until),
        # and country = ? (Spark also offers an equality with a constant; pushFilters serves
        # it). Spark evaluates every unselected candidate on the returned rows.
        chosen = [i for i in chosen if i is not None]
        if not chosen or candidates[chosen[0]].attribute != ("user_id",):
            return []  # no access path fits: the query fails
        self.on = [(candidates[i].attribute[0], SQL_OPS[type(candidates[i])]) for i in chosen]
        return chosen

    def maxParameterRowsPerRequest(self):
        # Override the default (1000, Spark's cap) to fit SQLite's "?" limit: each parameter
        # row takes one marker per selected predicate plus one for its ordinal, and the pushed
        # country filter takes one more.
        fixed = 1 if self.country is not None else 0
        return (SQLITE_MAX_MARKERS - fixed) // (len(self.on) + 1)

    def _query(self, parameters):
        # ONE statement per request, not one per key: the parameter rows become a VALUES table
        # with their ordinals, joined to the table. A key with several matches returns several
        # rows. A stored row that matches several parameter rows comes back once per ordinal,
        # as the contract asks. (Spark's current rules never put two such rows in one request:
        # their equality keys differ, and a stream range sends one row per request.)
        if self.conn is None:
            self.conn = connect(self.file_name)
        keys = [f"k{i}" for i in range(len(self.on))]
        marks = "(" + ", ".join("?" * (len(keys) + 1)) + ")"
        cond = " AND ".join(f"o.{col} {op} p.{k}" for (col, op), k in zip(self.on, keys))
        args = [v for i, row in enumerate(parameters) for v in (i, *row)]
        if self.country is not None:
            cond += " AND o.country = ?"
            args.append(self.country)
        sql = (
            f"WITH p(ord, {', '.join(keys)}) AS (VALUES {', '.join([marks] * len(parameters))}) "
            f"SELECT p.ord, {', '.join('o.' + c for c in COLUMNS)} "
            f"FROM p JOIN offers o ON {cond}"
        )
        return self.conn.execute(sql, args)

    def lookup(self, parameters):
        # parameters: a list of tuples, one per distinct parameter row and one value per
        # selected predicate; ordinal i is parameters[i]. segment can be None, and IS matches a
        # NULL. An ordinal that never appears in the results is a miss.
        cursor = self._query(parameters)
        if not self.arrow:
            # Yield (ordinal, row in schema order) pairs.
            for row in cursor:
                yield row[0], row[1:]
            return
        # Or yield PyArrow RecordBatches, as read() may: column 0 is the ordinal, then the
        # columns of the schema, in order, with the Arrow types of the schema.
        import pyarrow as pa

        names = ["ordinal"] + COLUMNS
        types = [pa.int32(), pa.int64(), pa.string(), pa.int64(), pa.int64()]
        types += [pa.string(), pa.string()]
        while rows := cursor.fetchmany(1000):
            columns = zip(*rows)
            arrays = [pa.array(values, t) for values, t in zip(columns, types)]
            yield pa.RecordBatch.from_arrays(arrays, names=names)

    def close(self):
        # Called once per copy, after its last request, when the task ends normally.
        if self.conn is not None:
            self.conn.close()


def build_demo_files(root):
    """Writes the SQLite file of offers and a directory of events in `root`."""
    db = os.path.join(root, "offers.db")
    conn = sqlite3.connect(db)
    with conn:
        conn.execute(
            "CREATE TABLE offers (user_id INT, segment TEXT, valid_from INT, "
            "valid_until INT, country TEXT, offer TEXT)"
        )
        conn.executemany(
            "INSERT INTO offers VALUES (?, ?, ?, ?, ?, ?)",
            [
                (1, "gold", 1, 100, "KR", "gold-1"),
                (1, "gold", 5, 100, "KR", "gold-5"),  # user 1: several matches
                (2, None, 1, 100, "KR", "no-segment"),  # NULL key, matched by <=>
                (3, "gold", 1, 100, "US", "us-only"),  # dropped by country = 'KR'
                (4, "gold", 9, 100, "KR", "from-day-9"),  # not valid yet on day 6
            ],
        )
    conn.close()
    events = os.path.join(root, "events")
    os.mkdir(events)
    with open(os.path.join(events, "part-0.json"), "w") as f:
        for user_id, segment, day in [
            (1, "gold", 6),
            (1, "gold", 2),
            (2, None, 6),
            (3, "gold", 6),
            (4, "gold", 6),
            (5, "gold", 6),
        ]:
            f.write(json.dumps({"user_id": user_id, "segment": segment, "day": day}) + "\n")
    return db, events


def enrich_events(spark, events_dir, select_range=True, arrow=False):
    """
    Runs the lookup join of the events in `events_dir` with the offers, and returns the result
    as a DataFrame. The SQLite file must already be shipped to the executors.
    """
    spark.dataSource.register(OffersDataSource)
    offers = (
        spark.read.format("offers")
        .option("file", "offers.db")
        .option("select_range", str(select_range).lower())
        .option("arrow", str(arrow).lower())
        .load()
        .hint("EXTERNAL_LOOKUP")  # hint mode: ask for lookups
        .where("country = 'KR'")  # a lookup-side filter
    )
    events = spark.readStream.schema("user_id BIGINT, segment STRING, day BIGINT").json(events_dir)
    joined = events.join(
        offers,
        (offers.user_id == events.user_id)
        & offers.segment.eqNullSafe(events.segment)
        & (offers.valid_from <= events.day)
        & (events.day < offers.valid_until),
        "left",
    ).select(events.user_id, events.segment, events.day, offers.offer)

    query = (
        joined.writeStream.format("memory").queryName("enriched").trigger(availableNow=True).start()
    )
    query.awaitTermination()
    return spark.sql("SELECT * FROM enriched ORDER BY user_id, day, offer")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("PythonLookupJoin").getOrCreate()
    # Python filter pushdown is off by default; pushFilters needs it.
    spark.conf.set("spark.sql.python.filterPushdown.enabled", "true")
    db, events_dir = build_demo_files(tempfile.mkdtemp())
    # Ship the file to every executor. In Spark Connect: spark.addArtifacts(db, file=True).
    # With spark-submit, --files offers.db ships it as well.
    spark.sparkContext.addFile(db)
    enrich_events(spark, events_dir).show()
    spark.stop()

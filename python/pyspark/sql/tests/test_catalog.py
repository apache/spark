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
from pyspark import StorageLevel
from pyspark.errors import AnalysisException, PySparkTypeError
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.testing.sqlutils import ReusedSQLTestCase


class CatalogTestsMixin:
    def test_current_database(self):
        spark = self.spark
        with self.database("some_db"):
            self.assertEqual(spark.catalog.currentDatabase(), "default")
            spark.sql("CREATE DATABASE some_db")
            spark.catalog.setCurrentDatabase("some_db")
            self.assertEqual(spark.catalog.currentDatabase(), "some_db")
            self.assertRaisesRegex(
                AnalysisException,
                "does_not_exist",
                lambda: spark.catalog.setCurrentDatabase("does_not_exist"),
            )

    def test_list_databases(self):
        spark = self.spark
        with self.database("some_db"):
            databases = [db.name for db in spark.catalog.listDatabases()]
            self.assertEqual(databases, ["default"])
            spark.sql("CREATE DATABASE some_db")
            databases = [db.name for db in spark.catalog.listDatabases()]
            self.assertEqual(sorted(databases), ["default", "some_db"])
            databases = [db.name for db in spark.catalog.listDatabases("def*")]
            self.assertEqual(sorted(databases), ["default"])
            databases = [db.name for db in spark.catalog.listDatabases("def2*")]
            self.assertEqual(sorted(databases), [])

    def test_database_exists(self):
        # SPARK-36207: testing that database_exists returns correct boolean
        spark = self.spark
        with self.database("some_db"):
            self.assertFalse(spark.catalog.databaseExists("some_db"))
            spark.sql("CREATE DATABASE some_db")
            self.assertTrue(spark.catalog.databaseExists("some_db"))
            self.assertTrue(spark.catalog.databaseExists("spark_catalog.some_db"))
            self.assertFalse(spark.catalog.databaseExists("spark_catalog.some_db2"))

    def test_get_database(self):
        spark = self.spark
        with self.database("some_db"):
            spark.sql("CREATE DATABASE some_db")
            db = spark.catalog.getDatabase("spark_catalog.some_db")
            self.assertEqual(db.name, "some_db")
            self.assertEqual(db.catalog, "spark_catalog")

    def test_list_tables(self):
        from pyspark.sql.catalog import Table

        spark = self.spark
        with self.database("some_db"):
            spark.sql("CREATE DATABASE some_db")
            with self.table("tab1", "some_db.tab2", "tab3_via_catalog"):
                with self.tempView("temp_tab"):
                    self.assertEqual(spark.catalog.listTables(), [])
                    self.assertEqual(spark.catalog.listTables("some_db"), [])
                    spark.createDataFrame([(1, 1)]).createOrReplaceTempView("temp_tab")
                    spark.sql("CREATE TABLE tab1 (name STRING, age INT) USING parquet")
                    spark.sql("CREATE TABLE some_db.tab2 (name STRING, age INT) USING parquet")

                    schema = StructType([StructField("a", IntegerType(), True)])
                    description = "this a table created via Catalog.createTable()"

                    with self.assertRaisesRegex(PySparkTypeError, "should be a struct type"):
                        # Test deprecated API and negative error case.
                        spark.catalog.createExternalTable(
                            "invalid_table_creation", schema=IntegerType(), description=description
                        )

                    spark.catalog.createTable(
                        "tab3_via_catalog", schema=schema, description=description
                    )

                    tables = sorted(spark.catalog.listTables(), key=lambda t: t.name)
                    tablesWithPattern = sorted(
                        spark.catalog.listTables(pattern="tab*"), key=lambda t: t.name
                    )
                    tablesDefault = sorted(
                        spark.catalog.listTables("default"), key=lambda t: t.name
                    )
                    tablesDefaultWithPattern = sorted(
                        spark.catalog.listTables("default", "tab*"), key=lambda t: t.name
                    )
                    tablesSomeDb = sorted(spark.catalog.listTables("some_db"), key=lambda t: t.name)
                    tablesSomeDbWithPattern = sorted(
                        spark.catalog.listTables("some_db", "tab*"), key=lambda t: t.name
                    )
                    self.assertEqual(tables, tablesDefault)
                    self.assertEqual(tablesWithPattern, tablesDefaultWithPattern)
                    self.assertEqual(len(tables), 3)
                    self.assertEqual(len(tablesWithPattern), 2)
                    self.assertEqual(len(tablesSomeDb), 2)
                    self.assertEqual(len(tablesSomeDbWithPattern), 1)

                    # make table in old fashion
                    def makeTable(
                        name,
                        database,
                        description,
                        tableType,
                        isTemporary,
                    ):
                        return Table(
                            name=name,
                            catalog=None,
                            namespace=[database] if database is not None else None,
                            description=description,
                            tableType=tableType,
                            isTemporary=isTemporary,
                        )

                    # compare tables in old fashion
                    def compareTables(t1, t2):
                        return (
                            t1.name == t2.name
                            and t1.database == t2.database
                            and t1.description == t2.description
                            and t1.tableType == t2.tableType
                            and t1.isTemporary == t2.isTemporary
                        )

                    self.assertTrue(
                        compareTables(
                            tables[0],
                            makeTable(
                                name="tab1",
                                database="default",
                                description=None,
                                tableType="MANAGED",
                                isTemporary=False,
                            ),
                        )
                    )
                    self.assertTrue(
                        compareTables(
                            tables[1],
                            makeTable(
                                name="tab3_via_catalog",
                                database="default",
                                description=description,
                                tableType="MANAGED",
                                isTemporary=False,
                            ),
                        )
                    )
                    self.assertTrue(
                        compareTables(
                            tables[2],
                            makeTable(
                                name="temp_tab",
                                database=None,
                                description=None,
                                tableType="TEMPORARY",
                                isTemporary=True,
                            ),
                        )
                    )
                    self.assertTrue(
                        compareTables(
                            tablesWithPattern[0],
                            makeTable(
                                name="tab1",
                                database="default",
                                description=None,
                                tableType="MANAGED",
                                isTemporary=False,
                            ),
                        )
                    )
                    self.assertTrue(
                        compareTables(
                            tablesWithPattern[1],
                            makeTable(
                                name="tab3_via_catalog",
                                database="default",
                                description=description,
                                tableType="MANAGED",
                                isTemporary=False,
                            ),
                        )
                    )
                    self.assertTrue(
                        compareTables(
                            tablesSomeDb[0],
                            makeTable(
                                name="tab2",
                                database="some_db",
                                description=None,
                                tableType="MANAGED",
                                isTemporary=False,
                            ),
                        )
                    )
                    self.assertTrue(
                        compareTables(
                            tablesSomeDb[1],
                            makeTable(
                                name="temp_tab",
                                database=None,
                                description=None,
                                tableType="TEMPORARY",
                                isTemporary=True,
                            ),
                        )
                    )
                    self.assertTrue(
                        compareTables(
                            tablesSomeDbWithPattern[0],
                            makeTable(
                                name="tab2",
                                database="some_db",
                                description=None,
                                tableType="MANAGED",
                                isTemporary=False,
                            ),
                        )
                    )
                    self.assertRaisesRegex(
                        AnalysisException,
                        "does_not_exist",
                        lambda: spark.catalog.listTables("does_not_exist"),
                    )

    def test_list_functions(self):
        spark = self.spark
        with self.database("some_db"):
            spark.sql("CREATE DATABASE some_db")
            functions = dict((f.name, f) for f in spark.catalog.listFunctions())
            functionsDefault = dict((f.name, f) for f in spark.catalog.listFunctions("default"))
            self.assertTrue(len(functions) > 200)
            self.assertTrue("+" in functions)
            self.assertTrue("like" in functions)
            self.assertTrue("month" in functions)
            self.assertTrue("to_date" in functions)
            self.assertTrue("to_timestamp" in functions)
            self.assertTrue("to_unix_timestamp" in functions)
            self.assertTrue("current_database" in functions)
            self.assertEqual(functions["+"].name, "+")
            self.assertEqual(functions["+"].description, "expr1 + expr2 - Returns `expr1`+`expr2`.")
            self.assertEqual(
                functions["+"].className, "org.apache.spark.sql.catalyst.expressions.Add"
            )
            self.assertTrue(functions["+"].isTemporary)
            self.assertEqual(functions, functionsDefault)

            functionsWithPattern = dict(
                (f.name, f) for f in spark.catalog.listFunctions(pattern="to*")
            )
            functionsDefaultWithPattern = dict(
                (f.name, f) for f in spark.catalog.listFunctions("default", "to*")
            )
            self.assertTrue(len(functionsWithPattern) > 10)
            self.assertFalse("+" in functionsWithPattern)
            self.assertFalse("like" in functionsWithPattern)
            self.assertFalse("month" in functionsWithPattern)
            self.assertTrue("to_date" in functionsWithPattern)
            self.assertTrue("to_timestamp" in functionsWithPattern)
            self.assertTrue("to_unix_timestamp" in functionsWithPattern)
            self.assertEqual(functionsWithPattern, functionsDefaultWithPattern)
            functionsWithPattern = dict(
                (f.name, f) for f in spark.catalog.listFunctions(pattern="*not_existing_func*")
            )
            self.assertTrue(len(functionsWithPattern) == 0)

            with self.function("func1", "some_db.func2"):
                try:
                    spark.udf
                    support_udf = True
                except Exception:
                    support_udf = False

                if support_udf:
                    spark.udf.register("temp_func", lambda x: str(x))
                spark.sql("CREATE FUNCTION func1 AS 'org.apache.spark.data.bricks'")
                spark.sql("CREATE FUNCTION some_db.func2 AS 'org.apache.spark.data.bricks'")
                newFunctions = dict((f.name, f) for f in spark.catalog.listFunctions())
                newFunctionsSomeDb = dict(
                    (f.name, f) for f in spark.catalog.listFunctions("some_db")
                )
                self.assertTrue(set(functions).issubset(set(newFunctions)))
                self.assertTrue(set(functions).issubset(set(newFunctionsSomeDb)))
                if support_udf:
                    self.assertTrue("temp_func" in newFunctions)
                self.assertTrue("func1" in newFunctions)
                self.assertTrue("func2" not in newFunctions)
                if support_udf:
                    self.assertTrue("temp_func" in newFunctionsSomeDb)
                self.assertTrue("func1" not in newFunctionsSomeDb)
                self.assertTrue("func2" in newFunctionsSomeDb)
                self.assertRaisesRegex(
                    AnalysisException,
                    "does_not_exist",
                    lambda: spark.catalog.listFunctions("does_not_exist"),
                )

    def test_function_exists(self):
        # SPARK-36258: testing that function_exists returns correct boolean
        spark = self.spark
        with self.function("func1"):
            self.assertFalse(spark.catalog.functionExists("func1"))
            self.assertFalse(spark.catalog.functionExists("default.func1"))
            self.assertFalse(spark.catalog.functionExists("spark_catalog.default.func1"))
            self.assertFalse(spark.catalog.functionExists("func1", "default"))
            spark.sql("CREATE FUNCTION func1 AS 'org.apache.spark.data.bricks'")
            self.assertTrue(spark.catalog.functionExists("func1"))
            self.assertTrue(spark.catalog.functionExists("default.func1"))
            self.assertTrue(spark.catalog.functionExists("spark_catalog.default.func1"))
            self.assertTrue(spark.catalog.functionExists("func1", "default"))

    def test_get_function(self):
        spark = self.spark
        with self.function("func1"):
            spark.sql("CREATE FUNCTION func1 AS 'org.apache.spark.data.bricks'")
            func1 = spark.catalog.getFunction("spark_catalog.default.func1")
            self.assertTrue(func1.name == "func1")
            self.assertTrue(func1.namespace == ["default"])
            self.assertTrue(func1.catalog == "spark_catalog")
            self.assertTrue(func1.className == "org.apache.spark.data.bricks")
            self.assertFalse(func1.isTemporary)

    def test_list_columns(self):
        from pyspark.sql.catalog import Column

        spark = self.spark
        with self.database("some_db"):
            spark.sql("CREATE DATABASE some_db")
            with self.table("tab1", "some_db.tab2"):
                spark.sql("CREATE TABLE tab1 (name STRING, age INT) USING parquet")
                spark.sql(
                    "CREATE TABLE some_db.tab2 (nickname STRING, tolerance FLOAT) USING parquet"
                )
                columns = sorted(
                    spark.catalog.listColumns("spark_catalog.default.tab1"), key=lambda c: c.name
                )
                columnsDefault = sorted(
                    spark.catalog.listColumns("tab1", "default"), key=lambda c: c.name
                )
                self.assertEqual(columns, columnsDefault)
                self.assertEqual(len(columns), 2)
                self.assertEqual(
                    columns[0],
                    Column(
                        name="age",
                        description=None,
                        dataType="int",
                        nullable=True,
                        isPartition=False,
                        isBucket=False,
                        isCluster=False,
                    ),
                )
                self.assertEqual(
                    columns[1],
                    Column(
                        name="name",
                        description=None,
                        dataType="string",
                        nullable=True,
                        isPartition=False,
                        isBucket=False,
                        isCluster=False,
                    ),
                )
                columns2 = sorted(
                    spark.catalog.listColumns("tab2", "some_db"), key=lambda c: c.name
                )
                self.assertEqual(len(columns2), 2)
                self.assertEqual(
                    columns2[0],
                    Column(
                        name="nickname",
                        description=None,
                        dataType="string",
                        nullable=True,
                        isPartition=False,
                        isBucket=False,
                        isCluster=False,
                    ),
                )
                self.assertEqual(
                    columns2[1],
                    Column(
                        name="tolerance",
                        description=None,
                        dataType="float",
                        nullable=True,
                        isPartition=False,
                        isBucket=False,
                        isCluster=False,
                    ),
                )
                self.assertRaisesRegex(
                    AnalysisException, "tab2", lambda: spark.catalog.listColumns("tab2")
                )
                self.assertRaisesRegex(
                    AnalysisException,
                    "does_not_exist",
                    lambda: spark.catalog.listColumns("does_not_exist"),
                )

    def test_table_cache(self):
        spark = self.spark
        with self.database("some_db"):
            spark.sql("CREATE DATABASE some_db")
            with self.table("tab1"):
                spark.sql("CREATE TABLE some_db.tab1 (name STRING, age INT) USING parquet")

                def if_cached(x):
                    return spark.catalog.isCached(x)

                names = ["some_db.tab1", "spark_catalog.some_db.tab1"]

                def assert_cached(c: bool):
                    if c:
                        self.assertTrue(all(map(if_cached, names)))
                    else:
                        self.assertFalse(any(map(if_cached, names)))

                assert_cached(False)
                spark.catalog.cacheTable("spark_catalog.some_db.tab1")
                assert_cached(True)
                spark.catalog.uncacheTable("spark_catalog.some_db.tab1")
                assert_cached(False)
                spark.catalog.cacheTable("spark_catalog.some_db.tab1", StorageLevel.MEMORY_ONLY)
                assert_cached(True)
                spark.catalog.clearCache()
                assert_cached(False)

    def test_table_exists(self):
        # SPARK-36176: testing that table_exists returns correct boolean
        spark = self.spark
        with self.database("some_db"):
            spark.sql("CREATE DATABASE some_db")
            with self.table("tab1", "some_db.tab2"):
                self.assertFalse(spark.catalog.tableExists("tab1"))
                self.assertFalse(spark.catalog.tableExists("tab2", "some_db"))
                spark.sql("CREATE TABLE tab1 (name STRING, age INT) USING parquet")
                self.assertTrue(spark.catalog.tableExists("tab1"))
                self.assertTrue(spark.catalog.tableExists("default.tab1"))
                self.assertTrue(spark.catalog.tableExists("spark_catalog.default.tab1"))
                self.assertTrue(spark.catalog.tableExists("tab1", "default"))
                spark.sql("CREATE TABLE some_db.tab2 (name STRING, age INT) USING parquet")
                self.assertFalse(spark.catalog.tableExists("tab2"))
                self.assertTrue(spark.catalog.tableExists("some_db.tab2"))
                self.assertTrue(spark.catalog.tableExists("spark_catalog.some_db.tab2"))
                self.assertTrue(spark.catalog.tableExists("tab2", "some_db"))

    def test_get_table(self):
        spark = self.spark
        with self.database("some_db"):
            spark.sql("CREATE DATABASE some_db")
            with self.table("tab1"):
                spark.sql("CREATE TABLE tab1 (name STRING, age INT) USING parquet")
                self.assertEqual(spark.catalog.getTable("tab1").database, "default")
                self.assertEqual(spark.catalog.getTable("default.tab1").catalog, "spark_catalog")
                self.assertEqual(spark.catalog.getTable("spark_catalog.default.tab1").name, "tab1")

    def test_refresh_table(self):
        import os
        import tempfile

        spark = self.spark
        with tempfile.TemporaryDirectory(prefix="test_refresh_table") as tmp_dir:
            with self.table("my_tab"):
                spark.sql(
                    "CREATE TABLE my_tab (col STRING) USING TEXT LOCATION '{}'".format(tmp_dir)
                )
                spark.sql("INSERT INTO my_tab SELECT 'abc'")
                spark.catalog.cacheTable("my_tab")
                self.assertEqual(spark.table("my_tab").count(), 1)

                os.system("rm -rf {}/*".format(tmp_dir))
                self.assertEqual(spark.table("my_tab").count(), 1)

                spark.catalog.refreshTable("spark_catalog.default.my_tab")
                self.assertEqual(spark.table("my_tab").count(), 0)


class CatalogTests(CatalogTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_catalog import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

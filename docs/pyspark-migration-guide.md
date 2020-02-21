---
layout: global
title: "Migration Guide: PySpark (Python on Spark)"
displayTitle: "Migration Guide: PySpark (Python on Spark)"
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

Note that this migration guide describes the items specific to PySpark.
Many items of SQL migration can be applied when migrating PySpark to higher versions.
Please refer [Migration Guide: SQL, Datasets and DataFrame](sql-migration-guide.html).

## Upgrading from PySpark 2.4 to 3.0

  - Since Spark 3.0, PySpark requires a Pandas version of 0.23.2 or higher to use Pandas related functionality, such as `toPandas`, `createDataFrame` from Pandas DataFrame, etc.

  - Since Spark 3.0, PySpark requires a PyArrow version of 0.12.1 or higher to use PyArrow related functionality, such as `pandas_udf`, `toPandas` and `createDataFrame` with "spark.sql.execution.arrow.enabled=true", etc.

  - In PySpark, when creating a `SparkSession` with `SparkSession.builder.getOrCreate()`, if there is an existing `SparkContext`, the builder was trying to update the `SparkConf` of the existing `SparkContext` with configurations specified to the builder, but the `SparkContext` is shared by all `SparkSession`s, so we should not update them. Since 3.0, the builder comes to not update the configurations. This is the same behavior as Java/Scala API in 2.3 and above. If you want to update them, you need to update them prior to creating a `SparkSession`.

  - In PySpark, when Arrow optimization is enabled, if Arrow version is higher than 0.11.0, Arrow can perform safe type conversion when converting Pandas.Series to Arrow array during serialization. Arrow will raise errors when detecting unsafe type conversion like overflow. Setting `spark.sql.execution.pandas.convertToArrowArraySafely` to true can enable it. The default setting is false. PySpark's behavior for Arrow versions is illustrated in the table below:
    <table class="table">
        <tr>
          <th>
            <b>PyArrow version</b>
          </th>
          <th>
            <b>Integer Overflow</b>
          </th>
          <th>
            <b>Floating Point Truncation</b>
          </th>
        </tr>
        <tr>
          <td>
            version < 0.11.0
          </td>
          <td>
            Raise error
          </td>
          <td>
            Silently allows
          </td>
        </tr>
        <tr>
          <td>
            version > 0.11.0, arrowSafeTypeConversion=false
          </td>
          <td>
            Silent overflow
          </td>
          <td>
            Silently allows
          </td>
        </tr>
        <tr>
          <td>
            version > 0.11.0, arrowSafeTypeConversion=true
          </td>
          <td>
            Raise error
          </td>
          <td>
            Raise error
          </td>
        </tr>
    </table>

  - Since Spark 3.0, `createDataFrame(..., verifySchema=True)` validates `LongType` as well in PySpark. Previously, `LongType` was not verified and resulted in `None` in case the value overflows. To restore this behavior, `verifySchema` can be set to `False` to disable the validation.

  - Since Spark 3.0, `Column.getItem` is fixed such that it does not call `Column.apply`. Consequently, if `Column` is used as an argument to `getItem`, the indexing operator should be used.
    For example, `map_col.getItem(col('id'))` should be replaced with `map_col[col('id')]`.

  - As of Spark 3.0 `Row` field names are no longer sorted alphabetically when constructing with named arguments for Python versions 3.6 and above, and the order of fields will match that as entered. To enable sorted fields by default, as in Spark 2.4, set the environment variable `PYSPARK_ROW_FIELD_SORTING_ENABLED` to "true" for both executors and driver - this environment variable must be consistent on all executors and driver; otherwise, it may cause failures or incorrect answers. For Python versions less than 3.6, the field names will be sorted alphabetically as the only option.

## Upgrading from PySpark 2.3 to 2.4

  - In PySpark, when Arrow optimization is enabled, previously `toPandas` just failed when Arrow optimization is unable to be used whereas `createDataFrame` from Pandas DataFrame allowed the fallback to non-optimization. Now, both `toPandas` and `createDataFrame` from Pandas DataFrame allow the fallback by default, which can be switched off by `spark.sql.execution.arrow.fallback.enabled`.

## Upgrading from PySpark 2.3.0 to 2.3.1 and above

  - As of version 2.3.1 Arrow functionality, including `pandas_udf` and `toPandas()`/`createDataFrame()` with `spark.sql.execution.arrow.enabled` set to `True`, has been marked as experimental. These are still evolving and not currently recommended for use in production.

## Upgrading from PySpark 2.2 to 2.3

  - In PySpark, now we need Pandas 0.19.2 or upper if you want to use Pandas related functionalities, such as `toPandas`, `createDataFrame` from Pandas DataFrame, etc.

  - In PySpark, the behavior of timestamp values for Pandas related functionalities was changed to respect session timezone. If you want to use the old behavior, you need to set a configuration `spark.sql.execution.pandas.respectSessionTimeZone` to `False`. See [SPARK-22395](https://issues.apache.org/jira/browse/SPARK-22395) for details.

  - In PySpark, `na.fill()` or `fillna` also accepts boolean and replaces nulls with booleans. In prior Spark versions, PySpark just ignores it and returns the original Dataset/DataFrame.

  - In PySpark, `df.replace` does not allow to omit `value` when `to_replace` is not a dictionary. Previously, `value` could be omitted in the other cases and had `None` by default, which is counterintuitive and error-prone.

## Upgrading from PySpark 1.4 to 1.5

 - Resolution of strings to columns in Python now supports using dots (`.`) to qualify the column or
   access nested values. For example `df['table.column.nestedField']`. However, this means that if
   your column name contains any dots you must now escape them using backticks (e.g., ``table.`column.with.dots`.nested``).

 - DataFrame.withColumn method in PySpark supports adding a new column or replacing existing columns of the same name.


## Upgrading from PySpark 1.0-1.2 to 1.3

#### Python DataTypes No Longer Singletons
{:.no_toc}

When using DataTypes in Python you will need to construct them (i.e. `StringType()`) instead of
referencing a singleton.

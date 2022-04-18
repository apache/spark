..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


=====================
Supported pandas APIs
=====================

.. currentmodule:: pyspark.pandas

The following table shows the pandas APIs that implemented or non-implemented from pandas API on
Spark.

Some pandas APIs do not implement full parameters, so the third column shows missing parameters for
each API.

If there is non-implemented pandas API or parameter you want, you can create an `Apache Spark
JIRA <https://issues.apache.org/jira/projects/SPARK/summary>`__ to request or to contribute by your
own.

The API list is updated based on the `latest pandas official API
reference <https://pandas.pydata.org/docs/reference/index.html#>`__.

All implemented APIs listed here are distributed except the ones that requires the local
computation by design. For example, `DataFrame.to_numpy() <https://spark.apache.org
/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.
to_numpy.html>`__ requires to collect the data to the driver side.

Supported DataFrame APIs
------------------------

+--------------------------------------------+-------------+--------------------------------------+
| API                                        | Implemented | Missing parameters                   |
+============================================+=============+======================================+
| `T <https://spark.apache.org/doc           | O           |                                      |
| s/latest/api/python/reference/pyspark.pand |             |                                      |
| as/api/pyspark.pandas.DataFrame.T.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `abs <https://spark.apache.org/docs/       | O           |                                      |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.abs.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `add <https://spark.apache.org/docs/       | O           | ``axis``, ``level``, ``fill_value``  |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.add.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `add_pre                                   | O           |                                      |
| fix <https://spark.apache.org/docs/latest/ |             |                                      |
| api/python/reference/pyspark.pandas/api/py |             |                                      |
| spark.pandas.DataFrame.add_prefix.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `add_suf                                   | O           |                                      |
| fix <https://spark.apache.org/docs/latest/ |             |                                      |
| api/python/reference/pyspark.pandas/api/py |             |                                      |
| spark.pandas.DataFrame.add_suffix.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `agg <https://spark.apache.org/docs/       | O           |                                      |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.agg.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `aggre                                     | O           |                                      |
| gate <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.aggregate.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `align <https://spark.apache.org/docs/la   | O           | ``fill_value``, ``method``,          |
| test/api/python/reference/pyspark.pandas/a |             | ``limit``, ``fill_axis``             |
| pi/pyspark.pandas.DataFrame.align.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `all <https://spark.apache.org/docs/       | O           | ``level``                            |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.all.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `any <https://spark.apache.org/docs/       | O           | `skipna``, ``level``                 |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.any.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `append <https://spark.apache.org/docs/lat | O           |                                      |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.append.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `apply <https://spark.apache.org/docs/la   | O           | ``raw``, ``result_type``             |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.apply.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `app                                       | O           | ``na_action``                        |
| lymap <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.applymap.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| asfreq                                     | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| asof                                       | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `assign <https://spark.apache.org/docs/lat | O           |                                      |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.assign.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `astype <https://spark.apache.org/docs/lat | O           | ``copy``, ``errors``                 |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.astype.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `at <https://spark.apache.org/docs         | O           |                                      |
| /latest/api/python/reference/pyspark.panda |             |                                      |
| s/api/pyspark.pandas.DataFrame.at.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `a                                         | O           |                                      |
| t_time <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.at_time.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| attrs                                      | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `axes <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.axes.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `bac                                       | O           |                                      |
| kfill <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.backfill.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `between_tim                               | O           | ``inclusive``                        |
| e <https://spark.apache.org/docs/latest/ap |             |                                      |
| i/python/reference/pyspark.pandas/api/pysp |             |                                      |
| ark.pandas.DataFrame.between_time.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `bfill <https://spark.apache.org/docs/la   | O           |                                      |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.bfill.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `bool <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.bool.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| boxplot                                    | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `clip <https://spark.apache.org/docs/l     | O           | ``axis``, ``inplace``                |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.clip.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `c                                         | O           |                                      |
| olumns <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.columns.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| combine                                    | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| combine_first                              | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| compare                                    | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| convert_dtypes                             | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `copy <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.copy.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `corr <https://spark.apache.org/docs/l     | O           | ``min_periods``                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.corr.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| corrwith                                   | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `count <https://spark.apache.org/docs/la   | O           | ``level``                            |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.count.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `cov <https://spark.apache.org/docs/       | O           | ``ddof``                             |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.cov.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `cummax <https://spark.apache.org/docs/lat | O           | ``axis``                             |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.cummax.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `cummin <https://spark.apache.org/docs/lat | O           | ``axis``                             |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.cummin.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `c                                         | O           | ``axis``                             |
| umprod <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.cumprod.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `cumsum <https://spark.apache.org/docs/lat | O           | ``axis``                             |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.cumsum.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `des                                       | O           | ``include``, ``exclude``,            |
| cribe <https://spark.apache.org/docs/lates |             | ``datetime_is_numeric``              |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.describe.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `diff <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.diff.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `div <https://spark.apache.org/docs/       | O           | ``axis``, ``level``, ``fill_value``  |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.div.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `divide <https://spark.apache.org/docs/lat | O           | ``axis``, ``level``, ``fill_value``  |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.divide.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `dot <https://spark.apache.org/docs/       | O           |                                      |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.dot.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `drop <https://spark.apache.org/docs/l     | O           | ``index``, ``level``, ``inplace``,   |
| atest/api/python/reference/pyspark.pandas/ |             | ``errors``                           |
| api/pyspark.pandas.DataFrame.drop.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `drop_duplicates <h                        | O           | ``ignore_index``                     |
| ttps://spark.apache.org/docs/latest/api/p  |             |                                      |
| ython/reference/pyspark.pandas/api/pyspark |             |                                      |
| .pandas.DataFrame.drop_duplicates.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `dropl                                     | O           |                                      |
| evel <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.droplevel.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `dropna <https://spark.apache.org/docs/lat | O           |                                      |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.dropna.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `dtypes <https://spark.apache.org/docs/lat | O           |                                      |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.dtypes.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `duplica                                   | O           |                                      |
| ted <https://spark.apache.org/docs/latest/ |             |                                      |
| api/python/reference/pyspark.pandas/api/py |             |                                      |
| spark.pandas.DataFrame.duplicated.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `empty <https://spark.apache.org/docs/la   | O           |                                      |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.empty.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `eq <https://spark.apache.org/docs         | O           | ``axis``, ``level``                  |
| /latest/api/python/reference/pyspark.panda |             |                                      |
| s/api/pyspark.pandas.DataFrame.eq.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `equals <https://spark.apache.org/docs/lat | O           | ``axis``, ``level``                  |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.equals.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `eval <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.eval.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `ewm <https://spark.apache.org/docs/l      | O           | ``adjust``, ``ignore_na``, ``axis``, |
| atest/api/python/reference/pyspark.pandas/ |             | ``method``                           |
| api/pyspark.pandas.DataFrame.ewm.html>`__  |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `expan                                     | O           |                                      |
| ding <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.expanding.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `e                                         | O           | ``ignore_index``                     |
| xplode <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.explode.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `ffill <https://spark.apache.org/docs/la   | O           |                                      |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.ffill.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `fillna <https://spark.apache.org/docs/lat | O           | ``downcast``                         |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.fillna.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `filter <https://spark.apache.org/docs/lat | O           |                                      |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.filter.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `first <https://spark.apache.org/docs/la   | O           |                                      |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.first.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `f                                         | O           |                                      |
| irst_valid_index <ht                       |             |                                      |
| tps://spark.apache.org/docs/latest/api/pyt |             |                                      |
| hon/reference/pyspark.pandas/api/pyspark.p |             |                                      |
| andas.DataFrame.first_valid_index.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| flags                                      | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `flo                                       | O           | ``axis``, ``level``, ``fill_value``  |
| ordiv <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.floordiv.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `from_                                     | O           |                                      |
| dict <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.from_dict.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `from_record                               | O           |                                      |
| s <https://spark.apache.org/docs/latest/ap |             |                                      |
| i/python/reference/pyspark.pandas/api/pysp |             |                                      |
| ark.pandas.DataFrame.from_records.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `ge <https://spark.apache.org/docs         | O           | ``axis``, ``level``                  |
| /latest/api/python/reference/pyspark.panda |             |                                      |
| s/api/pyspark.pandas.DataFrame.ge.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `get <https://spark.apache.org/docs/       | O           |                                      |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.get.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `g                                         | O           |                                      |
| roupby <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.groupby.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `gt <https://spark.apache.org/docs         | O           | ``axis``, ``level``                  |
| /latest/api/python/reference/pyspark.panda |             |                                      |
| s/api/pyspark.pandas.DataFrame.gt.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `head <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.head.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `hist <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.hist.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `iat <https://spark.apache.org/docs/       | O           |                                      |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.iat.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `idxmax <https://spark.apache.org/docs/lat | O           | ``skipna``                           |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.idxmax.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `idxmin <https://spark.apache.org/docs/lat | O           | ``skipna``                           |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.idxmin.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `iloc <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.iloc.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `index <https://spark.apache.org/docs/la   | O           |                                      |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.index.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| infer_objects                              | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `info <https://spark.apache.org/docs/l     | O           | ``show_counts``                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.info.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `insert <https://spark.apache.org/docs/lat | O           |                                      |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.insert.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `interpolate <https://spark.apache.org/doc | O           | ``axis``, ``inplace``,               |
| s/latest/api/python/reference/pyspark.pand |             | ``limit_direction``, ``limit_area``, |
| as/api/pyspark.pandas.DataFrame.interpolat |             | ``downcast``                         |
| e.html>`__                                 |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `isin <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.isin.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `isna <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.isna.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `isnull <https://spark.apache.org/docs/lat | O           |                                      |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.isnull.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `items <https://spark.apache.org/docs/la   | O           |                                      |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.items.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `iteri                                     | O           |                                      |
| tems <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.iteritems.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `ite                                       | O           |                                      |
| rrows <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.iterrows.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `itertup                                   | O           |                                      |
| les <https://spark.apache.org/docs/latest/ |             |                                      |
| api/python/reference/pyspark.pandas/api/py |             |                                      |
| spark.pandas.DataFrame.itertuples.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `join <https://spark.apache.org/docs/l     | O           | ``sort``                             |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.join.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `keys <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.keys.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `kurt <https://spark.apache.org/docs/l     | O           | ``skipna``, ``level``                |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.kurt.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `kur                                       | O           | ``skipna``, ``level``                |
| tosis <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.kurtosis.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `last <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.last.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `last_valid_index <h                       | O           |                                      |
| ttps://spark.apache.org/docs/latest/api/py |             |                                      |
| thon/reference/pyspark.pandas/api/pyspark. |             |                                      |
| pandas.DataFrame.last_valid_index.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `le <https://spark.apache.org/docs         | O           | ``axis``, ``level``                  |
| /latest/api/python/reference/pyspark.panda |             |                                      |
| s/api/pyspark.pandas.DataFrame.le.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `loc <https://spark.apache.org/docs/       | O           |                                      |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.loc.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| lookup                                     | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `lt <https://spark.apache.org/docs         | O           | ``axis``, ``level``                  |
| /latest/api/python/reference/pyspark.panda |             |                                      |
| s/api/pyspark.pandas.DataFrame.lt.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `mad <https://spark.apache.org/docs/       | O           | ``skipna``, ``level``                |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.mad.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `mask <https://spark.apache.org/docs/l     | O           | ``inplace``, ``axis``, ``level``,    |
| atest/api/python/reference/pyspark.pandas/ |             | ``errors``                           |
| api/pyspark.pandas.DataFrame.mask.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `max <https://spark.apache.org/docs/       | O           | ``skipna``, ``level``                |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.max.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `mean <https://spark.apache.org/docs/l     | O           | ``skipna``, ``level``                |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.mean.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `median <https://spark.apache.org/docs/lat | O           | ``skipna``, ``level``                |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.median.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `melt <https://spark.apache.org/docs/l     | O           | ``col_level``, ``ignore_index``      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.melt.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| memory_usage                               | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `merge <https://spark.apache.org/docs/la   | O           | ``sort``, ``copy``, ``indicator``,   |
| test/api/python/reference/pyspark.pandas/a |             | ``validate``                         |
| pi/pyspark.pandas.DataFrame.merge.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `min <https://spark.apache.org/docs/       | O           | ``skipna``, ``level``                |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.min.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `mod <https://spark.apache.org/docs/       | O           | ``axis``, ``level``, ``fill_value``  |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.mod.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| mode                                       | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `mul <https://spark.apache.org/docs/       | O           | ``axis``, ``level``, ``fill_value``  |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.mul.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `mul                                       | O           | ``axis``, ``level``, ``fill_value``  |
| tiply <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.multiply.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `ndim <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.ndim.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `ne <https://spark.apache.org/docs         | O           | ``axis``, ``level``                  |
| /latest/api/python/reference/pyspark.panda |             |                                      |
| s/api/pyspark.pandas.DataFrame.ne.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `nla                                       | O           |                                      |
| rgest <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.nlargest.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `notna <https://spark.apache.org/docs/la   | O           |                                      |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.notna.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `n                                         | O           |                                      |
| otnull <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.notnull.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `nsmal                                     | O           |                                      |
| lest <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.nsmallest.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `n                                         | O           |                                      |
| unique <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.nunique.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `pad <https://spark.apache.org/docs/       | O           |                                      |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.pad.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `pct_cha                                   | O           | ``fill_method``, ``limit``, ``freq`` |
| nge <https://spark.apache.org/docs/latest/ |             |                                      |
| api/python/reference/pyspark.pandas/api/py |             |                                      |
| spark.pandas.DataFrame.pct_change.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `pipe <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.pipe.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `pivot <https://spark.apache.org/docs/la   | O           |                                      |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.pivot.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `pivot_tab                                 | O           |                                      |
| le <https://spark.apache.org/docs/latest/a |             |                                      |
| pi/python/reference/pyspark.pandas/api/pys |             |                                      |
| park.pandas.DataFrame.pivot_table.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `plot.                                     | O           |                                      |
| area <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.plot.area.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `plo                                       | O           |                                      |
| t.bar <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.plot.bar.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `plot.                                     | O           |                                      |
| barh <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.plot.barh.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `plo                                       | O           |                                      |
| t.box <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.plot.box.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `plot.densit                               | O           |                                      |
| y <https://spark.apache.org/docs/latest/ap |             |                                      |
| i/python/reference/pyspark.pandas/api/pysp |             |                                      |
| ark.pandas.DataFrame.plot.density.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `plot.hexb                                 | O           |                                      |
| in <https://spark.apache.org/docs/latest/a |             |                                      |
| pi/python/reference/pyspark.pandas/api/pys |             |                                      |
| park.pandas.DataFrame.plot.hexbin.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `plot.                                     | O           |                                      |
| hist <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.plot.hist.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `plo                                       | O           |                                      |
| t.kde <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.plot.kde.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `plot.                                     | O           |                                      |
| line <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.plot.line.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `plo                                       | O           |                                      |
| t.pie <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.plot.pie.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `plot.scatte                               | O           |                                      |
| r <https://spark.apache.org/docs/latest/ap |             |                                      |
| i/python/reference/pyspark.pandas/api/pysp |             |                                      |
| ark.pandas.DataFrame.plot.scatter.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `pop <https://spark.apache.org/docs/       | O           |                                      |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.pop.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `pow <https://spark.apache.org/docs/       | O           | ``axis``, ``level``, ``fill_value``  |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.pow.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `prod <https://spark.apache.org/docs/l     | O           | ``skipna``, ``level``                |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.prod.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `p                                         | O           | ``skipna``, ``level``                |
| roduct <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.product.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `qua                                       | O           | ``interpolation``                    |
| ntile <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.quantile.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `query <https://spark.apache.org/docs/la   | O           |                                      |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.query.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `radd <https://spark.apache.org/docs/l     | O           | ``axis``, ``level``, ``fill_value``  |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.radd.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `rank <https://spark.apache.org/docs/l     | O           | ``axis``, ``na_options``, ``pct``    |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.rank.html>`__ |             |                                      |
|                                            |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `rdiv <https://spark.apache.org/docs/l     | O           | ``axis``, ``level``, ``fill_value``  |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.rdiv.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `r                                         | O           | ``method``, ``level``, ``limit``,    |
| eindex <https://spark.apache.org/docs/late |             | ``tolerance``                        |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.reindex.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `reindex_lik                               | O           | ``method``, ``limit``, ``tolerance`` |
| e <https://spark.apache.org/docs/latest/ap |             |                                      |
| i/python/reference/pyspark.pandas/api/pysp |             |                                      |
| ark.pandas.DataFrame.reindex_like.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `rename <https://spark.apache.org/docs/lat | O           |                                      |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.rename.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `rename_ax                                 | O           | ``copy``                             |
| is <https://spark.apache.org/docs/latest/a |             |                                      |
| pi/python/reference/pyspark.pandas/api/pys |             |                                      |
| park.pandas.DataFrame.rename_axis.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| reorder_levels                             | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `r                                         | O           | ``regex``, ``method``                |
| eplace <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.replace.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| resample                                   | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `reset_ind                                 | O           |                                      |
| ex <https://spark.apache.org/docs/latest/a |             |                                      |
| pi/python/reference/pyspark.pandas/api/pys |             |                                      |
| park.pandas.DataFrame.reset_index.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `rfloo                                     | O           | ``axis``, ``level``, ``fill_value``  |
| rdiv <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.rfloordiv.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `rmod <https://spark.apache.org/docs/l     | O           | ``axis``, ``level``, ``fill_value``  |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.rmod.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `rmul <https://spark.apache.org/docs/l     | O           | ``axis``, ``level``, ``fill_value``  |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.rmul.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `r                                         | O           |                                      |
| olling <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.rolling.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `round <https://spark.apache.org/docs/la   | O           |                                      |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.round.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `rpow <https://spark.apache.org/docs/l     | O           | ``axis``, ``level``, ``fill_value``  |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.rpow.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `rsub <https://spark.apache.org/docs/l     | O           | ``axis``, ``level``, ``fill_value``  |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.rsub.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `rtr                                       | O           | ``axis``, ``level``, ``fill_value``  |
| uediv <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.rtruediv.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `sample <https://spark.apache.org/docs/lat | O           | ``weights``, ``axis``,               |
| est/api/python/reference/pyspark.pandas/ap |             | ``ignore_index``                     |
| i/pyspark.pandas.DataFrame.sample.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `select_dtypes <https://                   | O           |                                      |
| spark.apache.org/docs/latest/api           |             |                                      |
| /python/reference/pyspark.pandas/api/pyspa |             |                                      |
| rk.pandas.DataFrame.select_dtypes.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `sem <https://spark.apache.org/docs/       | O           | ``skipna``                           |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.sem.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| set_axis                                   | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| set_flags                                  | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `set_i                                     | O           | ``verify_integrity``                 |
| ndex <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.set_index.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `shape <https://spark.apache.org/docs/la   | O           |                                      |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.shape.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `shift <https://spark.apache.org/docs/la   | O           | ``freq``, ``axis``                   |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.shift.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `size <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.size.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `skew <https://spark.apache.org/docs/l     | O           | ``skipna``, ``level``                |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.skew.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| slice_shift                                | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `sort_in                                   | O           | ``sort_remaining``,                  |
| dex <https://spark.apache.org/docs/latest/ |             | ``ignore_index``, ``key``            |
| api/python/reference/pyspark.pandas/api/py |             |                                      |
| spark.pandas.DataFrame.sort_index.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `sort_valu                                 | O           | ``keep``                             |
| es <https://spark.apache.org/docs/latest/a |             |                                      |
| pi/python/reference/pyspark.pandas/api/pys |             |                                      |
| park.pandas.DataFrame.sort_values.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| sparse                                     | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `s                                         | O           |                                      |
| queeze <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.squeeze.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `stack <https://spark.apache.org/docs/la   | O           | ``level``, ``dropna``                |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.stack.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `std <https://spark.apache.org/docs/       | O           | ``skipna``, ``level``                |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.std.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `style <https://spark.apache.org/docs/la   | O           |                                      |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.style.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `sub <https://spark.apache.org/docs/       | O           | ``axis``, ``level``, ``fill_value``  |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.sub.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `sub                                       | O           | ``axis``, ``level``, ``fill_value``  |
| tract <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.subtract.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `sum <https://spark.apache.org/docs/       | O           | ``skipna``, ``level``                |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.sum.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `swa                                       | O           |                                      |
| paxes <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.swapaxes.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `swapl                                     | O           |                                      |
| evel <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.swaplevel.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `tail <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.tail.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `take <https://spark.apache.org/docs/l     | O           |                                      |
| atest/api/python/reference/pyspark.pandas/ |             |                                      |
| api/pyspark.pandas.DataFrame.take.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `to_clipboar                               | O           |                                      |
| d <https://spark.apache.org/docs/latest/ap |             |                                      |
| i/python/reference/pyspark.pandas/api/pysp |             |                                      |
| ark.pandas.DataFrame.to_clipboard.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `to_csv <https://spark.apache.org/docs/lat | O           | ``encoding``, ``compression``,       |
| est/api/python/reference/pyspark.pandas/ap |             | ``quoting``, ``line_terminator``,    |
| i/pyspark.pandas.DataFrame.to_csv.html>`__ |             | ``cunksize`` and more. See the       |
|                                            |             | `pandas.DataFrame.t                  |
|                                            |             | o_csv <https://                      |
|                                            |             | pandas.pydata.org/docs/reference/    |
|                                            |             | api/pandas.DataFrame.to_csv.         |
|                                            |             | html>`__ and `pyspark.pandas.DataFra |
|                                            |             | me.to_csv <https://spark.apache.org  |
|                                            |             | /docs/latest/api/python/reference/py |
|                                            |             | spark.pandas/api/pyspark.pandas.Data |
|                                            |             | Frame.to_csv.html>`__ for detail.    |
+--------------------------------------------+-------------+--------------------------------------+
| `t                                         | O           |                                      |
| o_dict <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.to_dict.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `to_                                       | O           | ``storage_options``                  |
| excel <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.to_excel.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| to_feather                                 | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| to_gbq                                     | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| to_hdf                                     | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `t                                         | O           | ``encoding``                         |
| o_html <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.to_html.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `t                                         | O           | ``date_format``,                     |
| o_json <https://spark.apache.org/docs/late |             | ``double_precision``,                |
| st/api/python/reference/pyspark.pandas/api |             | ``force_ascii``, ``date_unit``,      | 
| /pyspark.pandas.DataFrame.to_json.html>`__ |             | ``default_handler`` and more. See th |
|                                            |             | e `pandas.DataFrame.to_json <https:  |
|                                            |             | //pandas.pydata.org/docs/reference/a |
|                                            |             | pi/pandas.DataFrame.to_json.htm      |
|                                            |             | l>`__ and `pyspark.pandas.DataFrame. |
|                                            |             | to_json <https://spark.apache.org/   |
|                                            |             | docs/latest/api/python/reference/pys |
|                                            |             | park.pandas/api/pyspark.pandas.DataF |
|                                            |             | rame.to_json.html>`__ for detail.    |
+--------------------------------------------+-------------+--------------------------------------+
| `to_                                       | O           | ``caption``, ``label``, ``position`` |
| latex <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.to_latex.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `to_markdo                                 | O           | ``storage_options``                  |
| wn <https://spark.apache.org/docs/latest/a |             |                                      |
| pi/python/reference/pyspark.pandas/api/pys |             |                                      |
| park.pandas.DataFrame.to_markdown.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `to_                                       | O           |                                      |
| numpy <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.to_numpy.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `to_parq                                   | O           | ``engine``, ``storage_options``      |
| uet <https://spark.apache.org/docs/latest/ |             |                                      |
| api/python/reference/pyspark.pandas/api/py |             |                                      |
| spark.pandas.DataFrame.to_parquet.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| to_period                                  | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| to_pickle                                  | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `to_reco                                   | O           |                                      |
| rds <https://spark.apache.org/docs/latest/ |             |                                      |
| api/python/reference/pyspark.pandas/api/py |             |                                      |
| spark.pandas.DataFrame.to_records.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| to_sql                                     | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| to_stata                                   | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `to_st                                     | O           |                                      |
| ring <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.to_string.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| to_timestamp                               | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| to_xarray                                  | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| to_xml                                     | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `trans                                     | O           |                                      |
| form <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.transform.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `trans                                     | O           | ``copy``                             |
| pose <https://spark.apache.org/docs/latest |             |                                      |
| /api/python/reference/pyspark.pandas/api/p |             |                                      |
| yspark.pandas.DataFrame.transpose.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `t                                         | O           | ``axis``, ``level``, ``fill_value``  |
| ruediv <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.truediv.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `tru                                       | O           |                                      |
| ncate <https://spark.apache.org/docs/lates |             |                                      |
| t/api/python/reference/pyspark.pandas/api/ |             |                                      |
| pyspark.pandas.DataFrame.truncate.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| tshift                                     | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| tz_convert                                 | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| tz_localize                                | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `u                                         | O           | ``level``, ``fill_value``            |
| nstack <https://spark.apache.org/docs/late |             |                                      |
| st/api/python/reference/pyspark.pandas/api |             |                                      |
| /pyspark.pandas.DataFrame.unstack.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `update <https://spark.apache.org/docs/lat | O           | ``filter_func``, ``errors``          |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.update.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| value_counts                               | X           |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `values <https://spark.apache.org/docs/lat | O           |                                      |
| est/api/python/reference/pyspark.pandas/ap |             |                                      |
| i/pyspark.pandas.DataFrame.values.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `var <https://spark.apache.org/docs/       | O           |                                      |
| latest/api/python/reference/pyspark.pandas |             |                                      |
| /api/pyspark.pandas.DataFrame.var.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `where <https://spark.apache.org/docs/la   | O           | ``inplace``, ``level``, ``errors``   |
| test/api/python/reference/pyspark.pandas/a |             |                                      |
| pi/pyspark.pandas.DataFrame.where.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+
| `xs <https://spark.apache.org/docs         | O           | ``drop_level``                       |
| /latest/api/python/reference/pyspark.panda |             |                                      |
| s/api/pyspark.pandas.DataFrame.xs.html>`__ |             |                                      |
+--------------------------------------------+-------------+--------------------------------------+

Supported I/O APIs
------------------

+-----------------------+--------------------+-----------------------------------------------------+
| API                   | Implemented        | Missing parameters                                  |
+=======================+====================+=====================================================+
| read_pickle           | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| DataFrame.to_pickle   | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `read_table <https:/  | O                  |                                                     |
| /spark.apache.org/doc |                    |                                                     |
| s/latest/api/python/r |                    |                                                     |
| eference/pyspark.pand |                    |                                                     |
| as/api/pyspark.pandas |                    |                                                     |
| .read_pickle.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `read_csv <http       | O                  | ``converters``, ``true_values``, ``false_values``,  |
| s://spark.apache.org/ |                    | ``skipinitialspace``, ``skiprows`` and more. See th |
| docs/latest/api/pytho |                    | e `pandas.read_csv <https://pandas.py               |
| n/reference/pyspark.p |                    | data.org/docs/reference/api/pandas.read_csv         |
| andas/api/pyspark.pan |                    | .html>`__ and `pyspark.pandas.read_csv <https://    |
| das.read_csv.html>`__ |                    | spark.apache.org/docs/latest/api/python/reference   |
|                       |                    | /pyspark.pandas/api/pyspark.pandas.read_csv         |
|                       |                    | .html>`__ for detail.                               |
+-----------------------+--------------------+-----------------------------------------------------+
| `DataFrame            | O                  |                                                     |
| .to_csv <https://spar |                    |                                                     |
| k.apache.org/docs/lat |                    |                                                     |
| est/api/python/refere |                    |                                                     |
| nce/pyspark.pandas/ap |                    |                                                     |
| i/pyspark.pandas.Data |                    |                                                     |
| Frame.to_csv.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| read_fwf              | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `read_                | O                  |                                                     |
| clipboard <https://sp |                    |                                                     |
| ark.apache.org/docs/l |                    |                                                     |
| atest/api/python/refe |                    |                                                     |
| rence/pyspark.pandas/ |                    |                                                     |
| api/pyspark.pandas.re |                    |                                                     |
| ad_clipboard.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `Da                   | O                  |                                                     |
| taFrame.to_clipboar   |                    |                                                     |
| d <https://spark.apac |                    |                                                     |
| he.org/docs/latest/ap |                    |                                                     |
| i/python/reference/py |                    |                                                     |
| spark.pandas/api/pysp |                    |                                                     |
| ark.pandas.DataFrame. |                    |                                                     |
| to_clipboard.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `read_excel <https:   | O                  | ``skiprows``, ``na_filter``, ``decimal``,           |
| //spark.apache.org/do |                    | ``skipfooter``, ``storage_options``                 |
| cs/latest/api/python/ |                    |                                                     |
| reference/pyspark.pan |                    |                                                     |
| das/api/pyspark.panda |                    |                                                     |
| s.read_excel.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `DataFrame.to_        | O                  | ``storage_options``                                 |
| excel <https://spark. |                    |                                                     |
| apache.org/docs/lates |                    |                                                     |
| t/api/python/referenc |                    |                                                     |
| e/pyspark.pandas/api/ |                    |                                                     |
| pyspark.pandas.DataFr |                    |                                                     |
| ame.to_excel.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `read_json <https     | O                  | ``orient``, ``typ``, ``dtype``, ``convert_axes``,   |
| ://spark.apache.org/d |                    | ``convert_dates`` and more. See the                 |
| ocs/latest/api/python |                    | `pandas.read_json <https://pandas.pydata.org/       |
| /reference/pyspark.pa |                    | docs/reference/api/pandas.read_json.html>`__ and `p |
| ndas/api/pyspark.pand |                    | yspark.pandas.read_json <https://                   |
| as.read_json.html>`__ |                    | spark.apache.org/docs/latest/api/python/reference   |
|                       |                    | /pyspark.pandas/api/pyspark.pandas.read_json        |
|                       |                    | .html>`__ for detail.                               |
+-----------------------+--------------------+-----------------------------------------------------+
| `DataFrame.t          | O                  | ``date_format``, ``double_precision``,              |
| o_json <https://spark |                    | ``force_ascii``, ``date_unit``,                     | 
| .apache.org/docs/late |                    | ``default_handler`` and more. See the `pandas.DataF |
| st/api/python/referen |                    | rame.to_json <https://pandas.pydata.org/docs/refere |
| ce/pyspark.pandas/api |                    | nce/api/pandas.DataFrame.to_json.html>`__ and `pysp |
| /pyspark.pandas.DataF |                    | ark.pandas.to_json <https://spark.apache.org/docs/l |
| rame.to_json.html>`__ |                    | atest/api/python/reference/pyspark.pandas/api/pyspa |
|                       |                    | rk.pandas.DataFrame.to_json.html>`__ for detail.    |
+-----------------------+--------------------+-----------------------------------------------------+
| `read_html <https     | O                  |                                                     |
| ://spark.apache.org/d |                    |                                                     |
| ocs/latest/api/python |                    |                                                     |
| /reference/pyspark.pa |                    |                                                     |
| ndas/api/pyspark.pand |                    |                                                     |
| as.read_html.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `DataFrame.t          | O                  | ``encoding``                                        |
| o_html <https://spark |                    |                                                     |
| .apache.org/docs/late |                    |                                                     |
| st/api/python/referen |                    |                                                     |
| ce/pyspark.pandas/api |                    |                                                     |
| /pyspark.pandas.DataF |                    |                                                     |
| rame.to_html.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| read_xml              | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| DataFrame.to_xml      | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `DataFrame.to_        | O                  | ``caption``, ``label``, ``position``                |
| latex <https://spark. |                    |                                                     |
| apache.org/docs/lates |                    |                                                     |
| t/api/python/referenc |                    |                                                     |
| e/pyspark.pandas/api/ |                    |                                                     |
| pyspark.pandas.DataFr |                    |                                                     |
| ame.to_latex.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| read_hdf              | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| read_feather          | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| DataFrame.to_feather  | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `r                    | O                  | ``engine``, ``storage_options``,                    |
| ead_parquet <https:// |                    | ``use_nullable_dtypes``                             |
| spark.apache.org/docs |                    |                                                     |
| /latest/api/python/re |                    |                                                     |
| ference/pyspark.panda |                    |                                                     |
| s/api/pyspark.pandas. |                    |                                                     |
| read_parquet.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `DataFrame.to_parq    | O                  | ``engine``, ``storage_options``                     |
| uet <https://spark.ap |                    |                                                     |
| ache.org/docs/latest/ |                    |                                                     |
| api/python/reference/ |                    |                                                     |
| pyspark.pandas/api/py |                    |                                                     |
| spark.pandas.DataFram |                    |                                                     |
| e.to_parquet.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `read_orc <http       | O                  |                                                     |
| s://spark.apache.org/ |                    |                                                     |
| docs/latest/api/pytho |                    |                                                     |
| n/reference/pyspark.p |                    |                                                     |
| andas/api/pyspark.pan |                    |                                                     |
| das.read_orc.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| read_sas              | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| read_spss             | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `read_                | O                  | ``coerce_float``, ``parse_dates``, ``chunksize``    |
| sql_table <https://sp |                    |                                                     |
| ark.apache.org/docs/l |                    |                                                     |
| atest/api/python/refe |                    |                                                     |
| rence/pyspark.pandas/ |                    |                                                     |
| api/pyspark.pandas.re |                    |                                                     |
| ad_sql_table.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `read_                | O                  | ``coerce_float``, ``params``, ``parse_dates``,      |
| sql_query <https://sp |                    | ``chunksize``, ``dtype``                            |
| ark.apache.org/docs/l |                    |                                                     |
| atest/api/python/refe |                    |                                                     |
| rence/pyspark.pandas/ |                    |                                                     |
| api/pyspark.pandas.re |                    |                                                     |
| ad_sql_query.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| `read_sql <http       | O                  | ``coerce_float``, ``params``, ``parse_dates``,      |
| s://spark.apache.org/ |                    | ``chunksize``                                       |
| docs/latest/api/pytho |                    |                                                     |
| n/reference/pyspark.p |                    |                                                     |
| andas/api/pyspark.pan |                    |                                                     |
| das.read_sql.html>`__ |                    |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| DataFrame.to_sql      | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| read_gbq              | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| read_stata            | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+
| DataFrame.to_stata    | X                  |                                                     |
+-----------------------+--------------------+-----------------------------------------------------+

Supported General Function APIs
-------------------------------

+-----------------------------------------+--------------+-----------------------------------------+
| API                                     | Implemented  | Missing parameters                      |
+=========================================+==============+=========================================+
| `melt <https://spark.apache.org/do      | O            | ``col_level``, ``ignore_index``         |
| cs/latest/api/python/reference/pyspark. |              |                                         |
| pandas/api/pyspark.pandas.melt.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| pivot                                   | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| pivot_table                             | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| crosstab                                | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| cut                                     | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| qcut                                    | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| `merge <https://spark.apache.org/doc    | O            | ``copy``, ``indicator``, ``validate``   |
| s/latest/api/python/reference/pyspark.p |              |                                         |
| andas/api/pyspark.pandas.merge.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| merge_ordered                           | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| `merge_                                 | O            |                                         |
| asof <https://spark.apache.org/docs/lat |              |                                         |
| est/api/python/reference/pyspark.pandas |              |                                         |
| /api/pyspark.pandas.merge_asof.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| `concat <https://spark.apache.org/docs  | O            | ``keys``, ``levels``, ``names``,        |
| /latest/api/python/reference/pyspark.pa |              | ``verify_integrity``, ``copy``          |
| ndas/api/pyspark.pandas.concat.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| `get                                    | O            |                                         |
| \_dumm                                  |              |                                         |
| ies <https://spark.apache.org/docs/late |              |                                         |
| st/api/python/reference/pyspark.pandas/ |              |                                         |
| api/pyspark.pandas.get_dummies.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| factorize                               | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| unique                                  | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| wide_to_long                            | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| `isna <https://spark.apache.org/do      | O            |                                         |
| cs/latest/api/python/reference/pyspark. |              |                                         |
| pandas/api/pyspark.pandas.isna.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| `isnull <https://spark.apache.org/docs  | O            |                                         |
| /latest/api/python/reference/pyspark.pa |              |                                         |
| ndas/api/pyspark.pandas.isnull.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| `notna <https://spark.apache.org/doc    | O            |                                         |
| s/latest/api/python/reference/pyspark.p |              |                                         |
| andas/api/pyspark.pandas.notna.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| `                                       | O            |                                         |
| notnull <https://spark.apache.org/docs/ |              |                                         |
| latest/api/python/reference/pyspark.pan |              |                                         |
| das/api/pyspark.pandas.notnull.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| `to                                     | O            | ``errors``, ``downcast``                |
| _num                                    |              |                                         |
| eric <https://spark.apache.org/docs/lat |              |                                         |
| est/api/python/reference/pyspark.pandas |              |                                         |
| /api/pyspark.pandas.to_numeric.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| `to                                     | O            | ``dayfirst``, ``yearfirst``, ``utc``,   |
| _datet                                  |              | ``exact``                               |
| ime <https://spark.apache.org/docs/late |              |                                         |
| st/api/python/reference/pyspark.pandas/ |              |                                         |
| api/pyspark.pandas.to_datetime.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| `date                                   | O            |                                         |
| _r                                      |              |                                         |
| ange <https://spark.apache.org/docs/lat |              |                                         |
| est/api/python/reference/pyspark.pandas |              |                                         |
| /api/pyspark.pandas.date_range.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| bdate_range                             | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| period_range                            | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| `timedelta_range <https://              | O            |                                         |
| spark.apache.org/docs/latest/a          |              |                                         |
| pi/python/reference/pyspark.pandas/api/ |              |                                         |
| pyspark.pandas.timedelta_range.html>`__ |              |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| infer_freq                              | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| interval_range                          | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+
| eval                                    | X            |                                         |
+-----------------------------------------+--------------+-----------------------------------------+

Supported Series APIs
---------------------

+------------------------+-------------------+-----------------------------------------------------+
| API                    | Implemented       | Missing parameters                                  |
+========================+===================+=====================================================+
| `T <https://           | O                 |                                                     |
| spark.apache.          |                   |                                                     |
| org/docs/latest/api/py |                   |                                                     |
| thon/reference/pyspark |                   |                                                     |
| .pandas/api/pyspark.pa |                   |                                                     |
| ndas.Series.T.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `abs <h                | O                 |                                                     |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.abs.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `add <h                | O                 |                                                     |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.add.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `add_prefix <https://  | O                 |                                                     |
| spark.apache.org/docs/ |                   |                                                     |
| latest/api/python/refe |                   |                                                     |
| rence/pyspark.pandas/a |                   |                                                     |
| pi/pyspark.pandas.Seri |                   |                                                     |
| es.add_prefix.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `add_suffix <https://  | O                 |                                                     |
| spark.apache.org/docs/ |                   |                                                     |
| latest/api/python/refe |                   |                                                     |
| rence/pyspark.pandas/a |                   |                                                     |
| pi/pyspark.pandas.Seri |                   |                                                     |
| es.add_suffix.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `agg <h                | O                 | ``axis``                                            |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.agg.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `aggregate <https:/    | O                 | ``axis``                                            |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.aggregate.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `align <htt            | O                 | ``level``, ``fill_value``, ``method``, ``limit``,   |
| ps://spark.apache.org/ |                   | ``fill_axis``                                       |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.align.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `all <h                | O                 | ``bool_only``, ``skipna``, ``level``                |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.all.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `any <h                | O                 | ``bool_only``, ``skipna``, ``level``                |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.any.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `append <http          | O                 |                                                     |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.append.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `apply <htt            | O                 | ``convert_dtype``                                   |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.apply.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `argmax <http          | O                 | ``axis``, ``skipna``                                |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.argmax.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `argmin <http          | O                 | ``axis``, ``skipna``                                |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.argmin.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `argsort <https        | O                 | ``axis``, ``kind``, ``order``                       |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.argsort.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| array                  | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| asfreq                 | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `asof <ht              | O                 | ``subset``                                          |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.asof.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `astype <http          | O                 | ``copy``, ``errors``                                |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.astype.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `at <h                 | O                 |                                                     |
| ttps://spark.apache.o  |                   |                                                     |
| rg/docs/latest/api/pyt |                   |                                                     |
| hon/reference/pyspark. |                   |                                                     |
| pandas/api/pyspark.pan |                   |                                                     |
| das.Series.at.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `at_time <https        | O                 |                                                     |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.at_time.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| attrs                  | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| autocorr               | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `axes <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.axes.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `backfill <https:      | O                 |                                                     |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.backfill.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `between <https        | O                 |                                                     |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.between.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `be                    | O                 | ``inclusive``                                       |
| tween_time <https://sp |                   |                                                     |
| ark.apache.org/docs/la |                   |                                                     |
| test/api/python/refere |                   |                                                     |
| nce/pyspark.pandas/api |                   |                                                     |
| /pyspark.pandas.Series |                   |                                                     |
| .between_time.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `bfill <htt            | O                 |                                                     |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.bfill.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `bool <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.bool.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `cat <h                | O                 |                                                     |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.cat.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `clip <ht              | O                 | ``axis``                                            |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.clip.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| combine                | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `comb                  | O                 |                                                     |
| ine_first <https://spa |                   |                                                     |
| rk.apache.org/docs/lat |                   |                                                     |
| est/api/python/referen |                   |                                                     |
| ce/pyspark.pandas/api/ |                   |                                                     |
| pyspark.pandas.Series. |                   |                                                     |
| combine_first.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `compare <https        | O                 | ``align_axis``                                      |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.compare.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| convert_dtypes         | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `copy <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.copy.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `corr <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.corr.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `count <htt            | O                 |                                                     |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.count.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `cov <h                | O                 | ``ddof``                                            |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.cov.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `cummax <http          | O                 | ``axis``                                            |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.cummax.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `cummin <http          | O                 | ``axis``                                            |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.cummin.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `cumprod <https        | O                 | ``axis``                                            |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.cumprod.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `cumsum <http          | O                 | ``axis``                                            |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.cumsum.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `describe <https:      | O                 | ``include``, ``exclude``, ``datetime_is_numeric``   |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.describe.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `diff <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.diff.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `div <h                | O                 | ``fill_value``, ``level``                           |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.div.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `divide <http          | O                 | ``fill_value``, ``level``                           |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.divide.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `divmod <http          | O                 | ``fill_value``, ``level``                           |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.divmod.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `dot <h                | O                 |                                                     |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.dot.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `drop <ht              | O                 | ``columns``, ``inplace``, ``errors``                |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.drop.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `drop_dup              | O                 |                                                     |
| licates <https://spark |                   |                                                     |
| .apache.org/docs/lates |                   |                                                     |
| t/api/python/reference |                   |                                                     |
| /pyspark.pandas/api/py |                   |                                                     |
| spark.pandas.Series.dr |                   |                                                     |
| op_duplicates.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `droplevel <https:/    | O                 | ``axis``                                            |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.droplevel.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `dropna <http          | O                 |                                                     |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.dropna.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `dt <h                 | O                 |                                                     |
| ttps://spark.apache.o  |                   |                                                     |
| rg/docs/latest/api/pyt |                   |                                                     |
| hon/reference/pyspark. |                   |                                                     |
| pandas/api/pyspark.pan |                   |                                                     |
| das.Series.dt.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `dtype <htt            | O                 |                                                     |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.dtype.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `dtypes <http          | O                 |                                                     |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.dtypes.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `duplicated <https://  | O                 |                                                     |
| spark.apache.org/docs/ |                   |                                                     |
| latest/api/python/refe |                   |                                                     |
| rence/pyspark.pandas/a |                   |                                                     |
| pi/pyspark.pandas.Seri |                   |                                                     |
| es.duplicated.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `empty <htt            | O                 |                                                     |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.empty.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `eq <h                 | O                 |                                                     |
| ttps://spark.apache.o  |                   |                                                     |
| rg/docs/latest/api/pyt |                   |                                                     |
| hon/reference/pyspark. |                   |                                                     |
| pandas/api/pyspark.pan |                   |                                                     |
| das.Series.eq.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `equals <http          | O                 |                                                     |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.equals.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `ewm <http             | O                 | ``adjust``, ``ignore_na``, ``axis``, ``method``     |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.ewm.html>`__    |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `expanding <https:/    | O                 |                                                     |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.expanding.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `explode <https        | O                 |                                                     |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.explode.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `factorize <https:/    | O                 |                                                     |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.factorize.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `ffill <htt            | O                 |                                                     |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.ffill.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `fillna <http          | O                 | ``downcast``                                        |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.fillna.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `filter <http          | O                 |                                                     |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.filter.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `first <htt            | O                 |                                                     |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.first.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `first_valid_          | O                 |                                                     |
| index <https://spark.a |                   |                                                     |
| pache.org/docs/latest/ |                   |                                                     |
| api/python/reference/p |                   |                                                     |
| yspark.pandas/api/pysp |                   |                                                     |
| ark.pandas.Series.firs |                   |                                                     |
| t_valid_index.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| flags                  | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `floordiv <https:      | O                 | ``fill_value``, ``level``                           |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.floordiv.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `ge <h                 | O                 | ``fill_value``, ``level``                           |
| ttps://spark.apache.o  |                   |                                                     |
| rg/docs/latest/api/pyt |                   |                                                     |
| hon/reference/pyspark. |                   |                                                     |
| pandas/api/pyspark.pan |                   |                                                     |
| das.Series.ge.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `get <h                | O                 |                                                     |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.get.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `groupby <https        | O                 |                                                     |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.groupby.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `gt <h                 | O                 | ``fill_value``, ``level``                           |
| ttps://spark.apache.o  |                   |                                                     |
| rg/docs/latest/api/pyt |                   |                                                     |
| hon/reference/pyspark. |                   |                                                     |
| pandas/api/pyspark.pan |                   |                                                     |
| das.Series.gt.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `hasnans <https        | O                 |                                                     |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.hasnans.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `head <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.head.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `hist <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.hist.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `iat <h                | O                 |                                                     |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.iat.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `idxmax <http          | O                 | ``axis``                                            |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.idxmax.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `idxmin <http          | O                 | ``axis``                                            |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.idxmin.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `iloc <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.iloc.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `index <htt            | O                 |                                                     |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.index.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| infer_objects          | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `interpolate <https:// | O                 | ``axis``, ``inplace``, ``limit_direction``,         |
| spark.apache.          |                   | ``limit_area``, ``downcast``                        |
| org/docs/latest/api/py |                   |                                                     |
| thon/reference/pyspark |                   |                                                     |
| .pandas/api/pyspark.pa |                   |                                                     |
| ndas.Series.interpolat |                   |                                                     |
| e.html>`__             |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `is                    | O                 |                                                     |
| _monotonic <https://sp |                   |                                                     |
| ark.apache.org/docs/la |                   |                                                     |
| test/api/python/refere |                   |                                                     |
| nce/pyspark.pandas/api |                   |                                                     |
| /pyspark.pandas.Series |                   |                                                     |
| .is_monotonic.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `is_monotonic_decrea   | O                 |                                                     |
| sing <https://         |                   |                                                     |
| spark.apache.          |                   |                                                     |
| org/docs/latest/api/py |                   |                                                     |
| thon/reference/pyspark |                   |                                                     |
| .pandas/api/pyspark.pa |                   |                                                     |
| ndas.Series.is_monoton |                   |                                                     |
| ic_decreasing.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `is_monotonic_increa   | O                 |                                                     |
| sing <https://         |                   |                                                     |
| spark.apache.          |                   |                                                     |
| org/docs/latest/api/py |                   |                                                     |
| thon/reference/pyspark |                   |                                                     |
| .pandas/api/pyspark.pa |                   |                                                     |
| ndas.Series.is_monoton |                   |                                                     |
| ic_increasing.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `is_unique <https:/    | O                 |                                                     |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.is_unique.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `isin <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.isin.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `isna <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.isna.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `isnull <http          | O                 |                                                     |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.isnull.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `item <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.item.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `items <htt            | O                 |                                                     |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.items.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `iteritems <https:/    | O                 |                                                     |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.iteritems.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `keys <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.keys.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `kurt <ht              | O                 | ``skipna``, ``level``                               |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.kurt.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `kurtosis <https:      | O                 | ``skipna``, ``level``                               |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.kurtosis.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `last <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.last.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `last_valid            | O                 |                                                     |
| _index <https://spark. |                   |                                                     |
| apache.org/docs/latest |                   |                                                     |
| /api/python/reference/ |                   |                                                     |
| pyspark.pandas/api/pys |                   |                                                     |
| park.pandas.Series.las |                   |                                                     |
| t_valid_index.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `le <h                 | O                 | ``fill_value``, ``level``                           |
| ttps://spark.apache.o  |                   |                                                     |
| rg/docs/latest/api/pyt |                   |                                                     |
| hon/reference/pyspark. |                   |                                                     |
| pandas/api/pyspark.pan |                   |                                                     |
| das.Series.le.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `loc <h                | O                 |                                                     |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.loc.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `lt <h                 | O                 | ``fill_value``, ``level``                           |
| ttps://spark.apache.o  |                   |                                                     |
| rg/docs/latest/api/pyt |                   |                                                     |
| hon/reference/pyspark. |                   |                                                     |
| pandas/api/pyspark.pan |                   |                                                     |
| das.Series.lt.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `mad <h                | O                 | ``axis``, ``skipna``, ``level``                     |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.mad.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `map <h                | O                 |                                                     |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.map.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `mask <ht              | O                 | ``inplace``, ``axis``, ``level``, ``errors``        |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.mask.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `max <h                | O                 | ``skipna``, ``level``                               |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.max.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `mean <ht              | O                 | ``skipna``, ``level``                               |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.mean.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `median <http          | O                 | ``skipna``, ``level``                               |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.median.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| memory_usage           | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `min <h                | O                 | ``skipna``, ``level``                               |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.min.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `mod <h                | O                 | ``fill_value``, ``level``                           |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.mod.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `mode <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.mode.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `mul <h                | O                 | ``fill_value``, ``level``                           |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.mul.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `multiply <https:      | O                 | ``fill_value``, ``level``                           |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.multiply.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `name <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.name.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| nbytes                 | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `ndim <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.ndim.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `ne <h                 | O                 | ``fill_value``, ``level``                           |
| ttps://spark.apache.o  |                   |                                                     |
| rg/docs/latest/api/pyt |                   |                                                     |
| hon/reference/pyspark. |                   |                                                     |
| pandas/api/pyspark.pan |                   |                                                     |
| das.Series.ne.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `nlargest <https:      | O                 | ``keep``                                            |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.nlargest.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `notna <htt            | O                 |                                                     |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.notna.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `notnull <https        | O                 |                                                     |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.notnull.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `nsmallest <https:/    | O                 | ``keep``                                            |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.nsmallest.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `nunique <https        | O                 |                                                     |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.nunique.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `pad <h                | O                 | ``downcast``                                        |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.pad.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `pct_change <https://  | O                 | ``fill_method``, ``limit``, ``freq``                |
| spark.apache.org/docs/ |                   |                                                     |
| latest/api/python/refe |                   |                                                     |
| rence/pyspark.pandas/a |                   |                                                     |
| pi/pyspark.pandas.Seri |                   |                                                     |
| es.pct_change.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `pipe <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.pipe.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `plot.area <https:/    | O                 |                                                     |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.plot.area.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `plot.bar <https:      | O                 |                                                     |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.plot.bar.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `plot.barh <https:/    | O                 |                                                     |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.plot.barh.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `plot.box <https:      | O                 |                                                     |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.plot.box.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `plot.density <https   | O                 |                                                     |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.density.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `plot.hist <https:/    | O                 |                                                     |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.plot.hist.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `plot.kde <https:      | O                 |                                                     |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.plot.kde.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `plot.line <https:/    | O                 |                                                     |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.plot.line.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `plot.pie <https:      | O                 |                                                     |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.plot.pie.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `pop <h                | O                 |                                                     |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.pop.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `pow <h                | O                 | ``fill_value``, ``level``                           |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.pow.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `prod <ht              | O                 | ``skipna``, ``level``                               |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.prod.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `product <https        | O                 | ``skipna``, ``level``                               |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.product.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `quantile <https:      | O                 | ``interpolation``                                   |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.quantile.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `radd <ht              | O                 | ``fill_value``, ``level``                           |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.radd.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `rank <ht              | O                 | ``axis``, ``na_option``, ``pct``                    |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.rank.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| ravel                  | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `rdiv <ht              | O                 | ``fill_value``, ``level``                           |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.rdiv.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `rdivmod <https        | O                 | ``fill_value``, ``level``                           |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.rdivmod.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `reindex <https        | O                 | ``method``, ``copy``, ``level``, ``limit``,         |
| ://spark.apache.org/do |                   | ``tolerance``                                       |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.reindex.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `re                    | O                 | ``method``, ``copy``, ``limit``, ``tolerance``      |
| index_like <https://sp |                   |                                                     |
| ark.apache.org/docs/la |                   |                                                     |
| test/api/python/refere |                   |                                                     |
| nce/pyspark.pandas/api |                   |                                                     |
| /pyspark.pandas.Series |                   |                                                     |
| .reindex_like.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `rename <http          | O                 | ``axis``                                            |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.rename.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `rename_axis <h        | O                 | ``axis``, ``copy``, ``inplace``                     |
| ttps://s               |                   |                                                     |
| park.apache.org/docs/l |                   |                                                     |
| atest/api/python/refer |                   |                                                     |
| ence/pyspark.pandas/ap |                   |                                                     |
| i/pyspark.pandas.Serie |                   |                                                     |
| s.rename_axis.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| reorder_levels         | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `repeat <http          | O                 | ``axis``                                            |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.repeat.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `replace <https        | O                 | ``inplace``, ``limit``, ``regex``, ``method``       |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.replace.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| resample               | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `reset_index <h        | O                 |                                                     |
| ttps://s               |                   |                                                     |
| park.apache.org/docs/l |                   |                                                     |
| atest/api/python/refer |                   |                                                     |
| ence/pyspark.pandas/ap |                   |                                                     |
| i/pyspark.pandas.Serie |                   |                                                     |
| s.reset_index.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `rfloordiv <https:/    | O                 | ``fill_value``, ``level``                           |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.rfloordiv.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `rmod <ht              | O                 | ``fill_value``, ``level``                           |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.rmod.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `rmul <ht              | O                 | ``fill_value``, ``level``                           |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.rmul.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `rolling <https        | O                 |                                                     |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.rolling.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `round <htt            | O                 |                                                     |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.round.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `rpow <ht              | O                 | ``fill_value``, ``level``                           |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.rpow.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `rsub <ht              | O                 | ``fill_value``, ``level``                           |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.rsub.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `rtruediv <https:      | O                 | ``fill_value``, ``level``                           |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.rtruediv.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `sample <http          | O                 | ``weight``, ``axis``                                |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.sample.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| searchsorted           | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `sem <h                | O                 | ``skipna``, ``level``                               |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.sem.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| set_axis               | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| set_flags              | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `shape <htt            | O                 |                                                     |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.shape.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `shift <htt            | O                 | ``freq``, ``axis``                                  |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.shift.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `size <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.size.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `skew <ht              | O                 | ``skipna``, ``level``                               |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.skew.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| slice_shift            | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `sort_index <https://  | O                 | ``sort_remaining``, ``ignore_index``, ``key``       |
| spark.apache.org/docs/ |                   |                                                     |
| latest/api/python/refe |                   |                                                     |
| rence/pyspark.pandas/a |                   |                                                     |
| pi/pyspark.pandas.Seri |                   |                                                     |
| es.sort_index.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `                      | O                 | ``axis``, ``kind``, ``key``                         |
| sort_values <https://s |                   |                                                     |
| park.apache.org/docs/l |                   |                                                     |
| atest/api/python/refer |                   |                                                     |
| ence/pyspark.pandas/ap |                   |                                                     |
| i/pyspark.pandas.Serie |                   |                                                     |
| s.sort_values.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| sparse                 | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `squeeze <https        | O                 |                                                     |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.squeeze.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `std <h                | O                 | ``skipna``, ``level``                               |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.std.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `str <h                | O                 |                                                     |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.std.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `sub <h                | O                 | ``fill_value``, ``level``                           |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.sub.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `subtract <https:      | O                 | ``fill_value``, ``level``                           |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.subtract.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `sum <h                | O                 | ``fill_value``, ``level``                           |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.sum.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `swaplevel <https:/    | O                 |                                                     |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.swaplevel.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `tail <ht              | O                 |                                                     |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.tail.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `take <ht              | O                 | ``axis``                                            |
| tps://spark.apache.org |                   |                                                     |
| /docs/latest/api/pytho |                   |                                                     |
| n/reference/pyspark.pa |                   |                                                     |
| ndas/api/pyspark.panda |                   |                                                     |
| s.Series.take.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `to                    | O                 |                                                     |
| _clipboard <https://sp |                   |                                                     |
| ark.apache.org/docs/la |                   |                                                     |
| test/api/python/refere |                   |                                                     |
| nce/pyspark.pandas/api |                   |                                                     |
| /pyspark.pandas.Series |                   |                                                     |
| .to_clipboard.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `to_csv <http          | O                 | ``encoding``, ``compression``, ``quoting``,         |
| s://spark.apache.org/d |                   | ``line_terminator``, ``cunksize`` and more. See the |
| ocs/latest/api/python/ |                   | `pandas.Series.to_csv <https://pandas.pydata.org/   |
| reference/pyspark.pand |                   | docs/reference/api/pandas.Series.to_csv.html>`__ an |
| as/api/pyspark.pandas. |                   | d `pyspark.pandas.Series.to_csv <https://           |
| Series.to_csv.html>`__ |                   | spark.apache.org/docs/latest/api/python/reference/p |
|                        |                   | yspark.pandas/api/pyspark.pandas.Series.to_csv.htm  |
|                        |                   | l>`__ for detail.                                   |
+------------------------+-------------------+-----------------------------------------------------+
| `to_dict <https        | O                 |                                                     |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.to_dict.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `to_excel <https:      | O                 | ``storage_options``                                 |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.to_excel.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `to_frame <https:      | O                 |                                                     |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.to_frame.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| to_hdf                 | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `to_json <https        | O                 | ``date_format``, ``double_precision``,              |
| ://spark.apache.org/do |                   | ``force_ascii``, ``default_handler``,               |
| cs/latest/api/python/r |                   | ``storage_options``                                 |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.to_json.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `to_latex <https:      | O                 | ``caption``, ``label``, ``position``                |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.to_latex.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `to_list <https        | O                 |                                                     |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.to_list.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `                      | O                 | ``storage_options``                                 |
| to_markdown <https://s |                   |                                                     |
| park.apache.org/docs/l |                   |                                                     |
| atest/api/python/refer |                   |                                                     |
| ence/pyspark.pandas/ap |                   |                                                     |
| i/pyspark.pandas.Serie |                   |                                                     |
| s.to_markdown.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `to_numpy <https:      | O                 |                                                     |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.to_numpy.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| to_period              | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| to_pickle              | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| to_sql                 | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `to_string <https:/    | O                 | ``min_rows``                                        |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.to_string.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| to_timestamp           | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| to_xarray              | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| tolist                 | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `transform <https:/    | O                 |                                                     |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.transform.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `transpose <https:/    | O                 |                                                     |
| /spark.apache.org/docs |                   |                                                     |
| /latest/api/python/ref |                   |                                                     |
| erence/pyspark.pandas/ |                   |                                                     |
| api/pyspark.pandas.Ser |                   |                                                     |
| ies.transpose.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `truediv <https        | O                 | ``fill_value``, ``level``                           |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.truediv.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `truncate <https:      | O                 |                                                     |
| //spark.apache.org/doc |                   |                                                     |
| s/latest/api/python/re |                   |                                                     |
| ference/pyspark.pandas |                   |                                                     |
| /api/pyspark.pandas.Se |                   |                                                     |
| ries.truncate.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| tshift                 | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| tz_convert             | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| tz_localize            | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `unique <http          | O                 |                                                     |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.unique.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `unstack <https        | O                 | ``fill_value``                                      |
| ://spark.apache.org/do |                   |                                                     |
| cs/latest/api/python/r |                   |                                                     |
| eference/pyspark.panda |                   |                                                     |
| s/api/pyspark.pandas.S |                   |                                                     |
| eries.unstack.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `update <http          | O                 |                                                     |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.update.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `va                    | O                 |                                                     |
| lue_counts <https://sp |                   |                                                     |
| ark.apache.org/docs/la |                   |                                                     |
| test/api/python/refere |                   |                                                     |
| nce/pyspark.pandas/api |                   |                                                     |
| /pyspark.pandas.Series |                   |                                                     |
| .value_counts.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `values <http          | O                 |                                                     |
| s://spark.apache.org/d |                   |                                                     |
| ocs/latest/api/python/ |                   |                                                     |
| reference/pyspark.pand |                   |                                                     |
| as/api/pyspark.pandas. |                   |                                                     |
| Series.values.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `var <h                | O                 | ``skipna``, ``level``                               |
| ttps://spark.apache.or |                   |                                                     |
| g/docs/latest/api/pyth |                   |                                                     |
| on/reference/pyspark.p |                   |                                                     |
| andas/api/pyspark.pand |                   |                                                     |
| as.Series.var.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| view                   | X                 |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `where <htt            | O                 | ``inplace``, ``axis``, ``level``, ``errors``        |
| ps://spark.apache.org/ |                   |                                                     |
| docs/latest/api/python |                   |                                                     |
| /reference/pyspark.pan |                   |                                                     |
| das/api/pyspark.pandas |                   |                                                     |
| .Series.where.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+
| `xs <h                 | O                 | ``axis``, ``drop_level``                            |
| ttps://spark.apache.o  |                   |                                                     |
| rg/docs/latest/api/pyt |                   |                                                     |
| hon/reference/pyspark. |                   |                                                     |
| pandas/api/pyspark.pan |                   |                                                     |
| das.Series.xs.html>`__ |                   |                                                     |
+------------------------+-------------------+-----------------------------------------------------+

Supported Index APIs
--------------------

+-----------------------------------------+-------------+-----------------------------------------+
| API                                     | Implemented | Missing parameters                      |
+=========================================+=============+=========================================+
| `T <https://spark.apache.org/docs/      | O           |                                         |
| latest/api/python/reference/pyspark.pan |             |                                         |
| das/api/pyspark.pandas.Index.T.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `all <https://spark.apache.org/docs/la  | O           |                                         |
| test/api/python/reference/pyspark.panda |             |                                         |
| s/api/pyspark.pandas.Index.all.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `any <https://spark.apache.org/docs/la  | O           |                                         |
| test/api/python/reference/pyspark.panda |             |                                         |
| s/api/pyspark.pandas.Index.any.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `appe                                   | O           |                                         |
| nd <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.append.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `argm                                   | O           | ``axis``                                |
| ax <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.argmax.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `argm                                   | O           | ``axis``                                |
| in <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.argmin.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| argsort                                 | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| array                                   | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `asi8 <https://                         | O           |                                         |
| spark.apache.org/docs/lat               |             |                                         |
| est/api/python/reference/pyspark.pandas |             |                                         |
| /api/pyspark.pandas.Index.asi8.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `asof <https://                         | O           |                                         |
| spark.apache.org/docs/lat               |             |                                         |
| est/api/python/reference/pyspark.pandas |             |                                         |
| /api/pyspark.pandas.Index.asof.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| asof_locs                               | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `asty                                   | O           | ``copy``                                |
| pe <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.astype.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `copy <https://                         | O           |                                         |
| spark.apache.org/docs/lat               |             |                                         |
| est/api/python/reference/pyspark.pandas |             |                                         |
| /api/pyspark.pandas.Index.copy.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `dele                                   | O           |                                         |
| te <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.delete.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `difference <h                          | O           |                                         |
| ttps://spark.apache.org/docs/latest/ap  |             |                                         |
| i/python/reference/pyspark.pandas/api/p |             |                                         |
| yspark.pandas.Index.difference.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `drop <https://                         | O           | ``errors``                              |
| spark.apache.org/docs/lat               |             |                                         |
| est/api/python/reference/pyspark.pandas |             |                                         |
| /api/pyspark.pandas.Index.drop.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `drop_duplicates <https                 | O           |                                         |
| ://spark.apache.org/docs/latest/api/pyt |             |                                         |
| hon/reference/pyspark.pandas/api/pyspar |             |                                         |
| k.pandas.Index.drop_duplicates.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `droplevel <https://                    | O           |                                         |
| spark.apache.org/docs/latest/a          |             |                                         |
| pi/python/reference/pyspark.pandas/api/ |             |                                         |
| pyspark.pandas.Index.droplevel.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `drop                                   | O           | ``how``                                 |
| na <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.dropna.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `dt                                     | O           |                                         |
| ype <https://spark.apache.org/docs/late |             |                                         |
| st/api/python/reference/pyspark.pandas/ |             |                                         |
| api/pyspark.pandas.Index.dtype.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| duplicated                              | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `em                                     | O           |                                         |
| pty <https://spark.apache.org/docs/late |             |                                         |
| st/api/python/reference/pyspark.pandas/ |             |                                         |
| api/pyspark.pandas.Index.empty.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `equa                                   | O           |                                         |
| ls <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.equals.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `factorize <https://                    | O           |                                         |
| spark.apache.org/docs/latest/a          |             |                                         |
| pi/python/reference/pyspark.pandas/api/ |             |                                         |
| pyspark.pandas.Index.factorize.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `fill                                   | O           | ``downcast``                            |
| na <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.fillna.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| format                                  | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| get_indexer                             | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| get_indexer_for                         | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| get_indexer_non_unique                  | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| get_level_values                        | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| get_loc                                 | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| get_slice_bound                         | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| get_value                               | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| groupby                                 | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `has_duplicates <http                   | O           |                                         |
| s://spark.apache.org/docs/latest/api/py |             |                                         |
| thon/reference/pyspark.pandas/api/pyspa |             |                                         |
| rk.pandas.Index.has_duplicates.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `hasnan                                 | O           |                                         |
| s <https://spark.apache.org/docs/latest |             |                                         |
| /api/python/reference/pyspark.pandas/ap |             |                                         |
| i/pyspark.pandas.Index.hasnans.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| holds_integer                           | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `identical <https://                    | O           |                                         |
| spark.apache.org/docs/latest/a          |             |                                         |
| pi/python/reference/pyspark.pandas/api/ |             |                                         |
| pyspark.pandas.Index.identical.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `inferred_type <htt                     | O           |                                         |
| ps://spark.apache.org/docs/latest/api/p |             |                                         |
| ython/reference/pyspark.pandas/api/pysp |             |                                         |
| ark.pandas.Index.inferred_type.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `inse                                   | O           |                                         |
| rt <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.insert.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `intersection <ht                       | O           | ``sort``                                |
| tps://spark.apache.org/docs/latest/api/ |             |                                         |
| python/reference/pyspark.pandas/api/pys |             |                                         |
| park.pandas.Index.intersection.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| is\_                                    | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_all_dates <ht                       | O           |                                         |
| tps://spark.apache.org/docs/latest/api/ |             |                                         |
| python/reference/pyspark.pandas/api/pys |             |                                         |
| park.pandas.Index.is_all_dates.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_boolean <h                          | O           |                                         |
| ttps://spark.apache.org/docs/latest/ap  |             |                                         |
| i/python/reference/pyspark.pandas/api/p |             |                                         |
| yspark.pandas.Index.is_boolean.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_categorical <http                   | O           |                                         |
| s://spark.apache.org/docs/latest/api/py |             |                                         |
| thon/reference/pyspark.pandas/api/pyspa |             |                                         |
| rk.pandas.Index.is_categorical.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_floating <h                         | O           |                                         |
| ttps://spark.apache.org/docs/latest/api |             |                                         |
| /python/reference/pyspark.pandas/api/py |             |                                         |
| spark.pandas.Index.is_floating.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_integer <h                          | O           |                                         |
| ttps://spark.apache.org/docs/latest/ap  |             |                                         |
| i/python/reference/pyspark.pandas/api/p |             |                                         |
| yspark.pandas.Index.is_integer.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_interval <h                         | O           |                                         |
| ttps://spark.apache.org/docs/latest/api |             |                                         |
| /python/reference/pyspark.pandas/api/py |             |                                         |
| spark.pandas.Index.is_interval.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| is_mixed                                | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_monotonic <ht                       | O           |                                         |
| tps://spark.apache.org/docs/latest/api/ |             |                                         |
| python/reference/pyspark.pandas/api/pys |             |                                         |
| park.pandas.Index.is_monotonic.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_monotonic_decreasing <https://spark | O           |                                         |
| .apache.org/docs/latest/api/python/refe |             |                                         |
| rence/pyspark.pandas/api/pyspark.pandas |             |                                         |
| .Index.is_monotonic_decreasing.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_monotonic_increasing <https://spark | O           |                                         |
| .apache.org/docs/latest/api/python/refe |             |                                         |
| rence/pyspark.pandas/api/pyspark.pandas |             |                                         |
| .Index.is_monotonic_increasing.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_numeric <h                          | O           |                                         |
| ttps://spark.apache.org/docs/latest/ap  |             |                                         |
| i/python/reference/pyspark.pandas/api/p |             |                                         |
| yspark.pandas.Index.is_numeric.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_object <https://                    | O           |                                         |
| spark.apache.org/docs/latest/a          |             |                                         |
| pi/python/reference/pyspark.pandas/api/ |             |                                         |
| pyspark.pandas.Index.is_object.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_type_compatible <https://           | O           |                                         |
| spark.apache.org/docs/latest/api/python |             |                                         |
| /reference/pyspark.pandas/api/pyspark.p |             |                                         |
| andas.Index.is_type_compatible.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `is_unique <https://                    | O           |                                         |
| spark.apache.org/docs/latest/a          |             |                                         |
| pi/python/reference/pyspark.pandas/api/ |             |                                         |
| pyspark.pandas.Index.is_unique.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `isin <https://                         | O           | ``level``                               |
| spark.apache.org/docs/lat               |             |                                         |
| est/api/python/reference/pyspark.pandas |             |                                         |
| /api/pyspark.pandas.Index.isin.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `isna <https://                         | O           |                                         |
| spark.apache.org/docs/lat               |             |                                         |
| est/api/python/reference/pyspark.pandas |             |                                         |
| /api/pyspark.pandas.Index.isna.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `isnu                                   | O           |                                         |
| ll <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.isnull.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `item <https://                         | O           |                                         |
| spark.apache.org/docs/lat               |             |                                         |
| est/api/python/reference/pyspark.pandas |             |                                         |
| /api/pyspark.pandas.Index.item.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| join                                    | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `map <https://spark.apache.org/docs/la  | O           |                                         |
| test/api/python/reference/pyspark.panda |             |                                         |
| s/api/pyspark.pandas.Index.map.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `max <https://spark.apache.org/docs/la  | O           | ``axis``, ``skipna``                    |
| test/api/python/reference/pyspark.panda |             |                                         |
| s/api/pyspark.pandas.Index.max.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| memory_usage                            | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `min <https://spark.apache.org/docs/la  | O           | ``axis``, ``skipna``                    |
| test/api/python/reference/pyspark.panda |             |                                         |
| s/api/pyspark.pandas.Index.min.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `na                                     | O           |                                         |
| me <https://spark.apache.org/docs/lat   |             |                                         |
| est/api/python/reference/pyspark.pandas |             |                                         |
| /api/pyspark.pandas.Index.name.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `na                                     | O           |                                         |
| mes <https://spark.apache.org/docs/late |             |                                         |
| st/api/python/reference/pyspark.pandas/ |             |                                         |
| api/pyspark.pandas.Index.names.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| nbytes                                  | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `nd                                     | O           |                                         |
| im <https://spark.apache.org/docs/lat   |             |                                         |
| est/api/python/reference/pyspark.pandas |             |                                         |
| /api/pyspark.pandas.Index.ndim.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `nlevel                                 | O           |                                         |
| s <https://spark.apache.org/docs/latest |             |                                         |
| /api/python/reference/pyspark.pandas/ap |             |                                         |
| i/pyspark.pandas.Index.nlevels.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `no                                     | O           |                                         |
| tna <https://spark.apache.org/docs/late |             |                                         |
| st/api/python/reference/pyspark.pandas/ |             |                                         |
| api/pyspark.pandas.Index.notna.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `notnul                                 | O           |                                         |
| l <https://spark.apache.org/docs/latest |             |                                         |
| /api/python/reference/pyspark.pandas/ap |             |                                         |
| i/pyspark.pandas.Index.notnull.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `nuniqu                                 | O           |                                         |
| e <https://spark.apache.org/docs/latest |             |                                         |
| /api/python/reference/pyspark.pandas/ap |             |                                         |
| i/pyspark.pandas.Index.nunique.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| putmask                                 | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| ravel                                   | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| reindex                                 | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `rena                                   | O           |                                         |
| me <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.rename.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `repe                                   | O           | ``axis``                                |
| at <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.repeat.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| searchsorted                            | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `set_names <https://                    | O           |                                         |
| spark.apache.org/docs/latest/a          |             |                                         |
| pi/python/reference/pyspark.pandas/api/ |             |                                         |
| pyspark.pandas.Index.set_names.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| set_value                               | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `sh                                     | O           |                                         |
| ape <https://spark.apache.org/docs/late |             |                                         |
| st/api/python/reference/pyspark.pandas/ |             |                                         |
| api/pyspark.pandas.Index.shape.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `sh                                     | O           | ``freq``                                |
| ift <https://spark.apache.org/docs/late |             |                                         |
| st/api/python/reference/pyspark.pandas/ |             |                                         |
| api/pyspark.pandas.Index.shift.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `si                                     | O           |                                         |
| ze <https://spark.apache.org/docs/lat   |             |                                         |
| est/api/python/reference/pyspark.pandas |             |                                         |
| /api/pyspark.pandas.Index.size.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| slice_indexer                           | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| slice_locs                              | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `sort_values <h                         | O           | ``na_position``, ``key``                |
| ttps://spark.apache.org/docs/latest/api |             |                                         |
| /python/reference/pyspark.pandas/api/py |             |                                         |
| spark.pandas.Index.sort_values.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| sortlevel                               | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `symmetric_difference <https://sp       | O           |                                         |
| ark.apache.org/docs/latest/api/python/r |             |                                         |
| eference/pyspark.pandas/api/pyspark.pan |             |                                         |
| das.Index.symmetric_difference.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `ta                                     | O           | ``axis``, ``allow_fill``,               |
| ke <https://spark.apache.org/docs/lat   |             | ``fill_value``                          |
| est/api/python/reference/pyspark.pandas |             |                                         |
| /api/pyspark.pandas.Index.take.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| to_flat_index                           | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `to_frame <https://                     | O           |                                         |
| spark.apache.org/docs/latest/           |             |                                         |
| api/python/reference/pyspark.pandas/api |             |                                         |
| /pyspark.pandas.Index.to_frame.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `to_lis                                 | O           |                                         |
| t <https://spark.apache.org/docs/latest |             |                                         |
| /api/python/reference/pyspark.pandas/ap |             |                                         |
| i/pyspark.pandas.Index.to_list.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| to_native_types                         | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `to_numpy <https://                     | O           |                                         |
| spark.apache.org/docs/latest/           |             |                                         |
| api/python/reference/pyspark.pandas/api |             |                                         |
| /pyspark.pandas.Index.to_numpy.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `to_series <https://                    | O           |                                         |
| spark.apache.org/docs/latest/a          |             |                                         |
| pi/python/reference/pyspark.pandas/api/ |             |                                         |
| pyspark.pandas.Index.to_series.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| tolist                                  | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| transpose                               | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `un                                     | O           |                                         |
| ion <https://spark.apache.org/docs/late |             |                                         |
| st/api/python/reference/pyspark.pandas/ |             |                                         |
| api/pyspark.pandas.Index.union.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `uniq                                   | O           |                                         |
| ue <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.unique.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `value_counts <ht                       | O           |                                         |
| tps://spark.apache.org/docs/latest/api/ |             |                                         |
| python/reference/pyspark.pandas/api/pys |             |                                         |
| park.pandas.Index.value_counts.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `valu                                   | O           |                                         |
| es <https://spark.apache.org/docs/lates |             |                                         |
| t/api/python/reference/pyspark.pandas/a |             |                                         |
| pi/pyspark.pandas.Index.values.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `vi                                     | O           |                                         |
| ew <https://spark.apache.org/docs/lat   |             |                                         |
| est/api/python/reference/pyspark.pandas |             |                                         |
| /api/pyspark.pandas.Index.view.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| where                                   | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+

Supported Window APIs
---------------------

+---------------------------------------------------------------+-------------+--------------------+
| API                                                           | Implemented | Missing parameters |
+===============================================================+=============+====================+
| rolling.agg                                                   | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.aggregate                                             | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.apply                                                 | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.axis                                                  | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.center                                                | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.closed                                                | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.corr                                                  | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| `rolling.count <https://                                      | O           |                    |
| spark.apache.org/docs/latest/api/python/reference/py          |             |                    |
| spark.pandas/api/pyspark.pandas.window.Rolling.count.html>`__ |             |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.cov                                                   | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.exclusions                                            | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling. is_datetimelike                                      | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.kurt                                                  | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| `rolling.ma                                                   | O           |                    |
| x <https://spark.apache.org/docs/latest/api/python/reference/ |             |                    |
| pyspark.pandas/api/pyspark.pandas.window.Rolling.max.html>`__ |             |                    |
+---------------------------------------------------------------+-------------+--------------------+
| `rolling.mean <https://                                       | O           |                    |
| spark.apache.org/docs/latest/api/python/reference/p           |             |                    |
| yspark.pandas/api/pyspark.pandas.window.Rolling.mean.html>`__ |             |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.median                                                | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.method                                                | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| `rolling.mi                                                   | O           |                    |
| n <https://spark.apache.org/docs/latest/api/python/reference/ |             |                    |
| pyspark.pandas/api/pyspark.pandas.window.Rolling.min.html>`__ |             |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.min_periods                                           | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.ndim                                                  | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.obj                                                   | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.on                                                    | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.quantile                                              | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.sem                                                   | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.skew                                                  | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.std                                                   | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| `rolling.su                                                   | O           |                    |
| m <https://spark.apache.org/docs/latest/api/python/reference/ |             |                    |
| pyspark.pandas/api/pyspark.pandas.window.Rolling.sum.html>`__ |             |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.validate                                              | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.var                                                   | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.win_type                                              | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| rolling.window                                                | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.agg                                                 | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.aggregate                                           | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.apply                                               | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.axis                                                | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.center                                              | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.closed                                              | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.corr                                                | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| `expanding.count <h                                           | O           |                    |
| ttps://spark.apache.org/docs/latest/api/python/reference/pysp |             |                    |
| ark.pandas/api/pyspark.pandas.window.Expanding.count.html>`__ |             |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.cov                                                 | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.exclusions                                          | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.is_datetimelike                                     | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.kurt                                                | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| `expanding.max <https://                                      | O           |                    |
| spark.apache.org/docs/latest/api/python/reference/py          |             |                    |
| spark.pandas/api/pyspark.pandas.window.Expanding.max.html>`__ |             |                    |
+---------------------------------------------------------------+-------------+--------------------+
| `expanding.mean <h                                            | O           |                    |
| ttps://spark.apache.org/docs/latest/api/python/reference/pys  |             |                    |
| park.pandas/api/pyspark.pandas.window.Expanding.mean.html>`__ |             |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.median                                              | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.method                                              | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| `expanding.min <https://                                      | O           |                    |
| spark.apache.org/docs/latest/api/python/reference/py          |             |                    |
| spark.pandas/api/pyspark.pandas.window.Expanding.min.html>`__ |             |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.min_periods                                         | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.ndim                                                | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.obj                                                 | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.on                                                  | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.quantile                                            | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.sem                                                 | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.skew                                                | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.std                                                 | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| `expanding.sum <https://                                      | O           |                    |
| spark.apache.org/docs/latest/api/python/reference/py          |             |                    |
| spark.pandas/api/pyspark.pandas.window.Expanding.sum.html>`__ |             |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.validate                                            | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.var                                                 | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.win_type                                            | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+
| expanding.window                                              | X           |                    |
+---------------------------------------------------------------+-------------+--------------------+

Supported GroupBy APIs
----------------------

+-----------------------------------------+-------------+-----------------------------------------+
| API                                     | Implemented | Missing parameters                      |
+=========================================+=============+=========================================+
| `agg <https://spark.apa                 | O           |                                         |
| che.org/docs/latest/api/python/referenc |             |                                         |
| e/pyspark.pandas/api/pyspark.pandas.gro |             |                                         |
| upby.DataFrameGroupBy.agg.html#pyspark. |             |                                         |
| pandas.groupby.DataFrameGroupBy.agg>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `aggregate <https://spark.apac          | O           |                                         |
| he.org/docs/latest/api/python/reference |             |                                         |
| /pyspark.pandas/api/pyspark.pandas.grou |             |                                         |
| pby.DataFrameGroupBy.aggregate.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `all <htt                               | O           | ``skipna``                              |
| ps://spark.apache.org/docs/latest/api/p |             |                                         |
| ython/reference/pyspark.pandas/api/pysp |             |                                         |
| ark.pandas.groupby.GroupBy.all.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `any <htt                               | O           | ``skipna``                              |
| ps://spark.apache.org/docs/latest/api/p |             |                                         |
| ython/reference/pyspark.pandas/api/pysp |             |                                         |
| ark.pandas.groupby.GroupBy.any.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `apply <https                           | O           |                                         |
| ://spark.apache.org/docs/latest/api/pyt |             |                                         |
| hon/reference/pyspark.pandas/api/pyspar |             |                                         |
| k.pandas.groupby.GroupBy.apply.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `backfill <https://                     | O           |                                         |
| spark.apache.org/docs/latest/api/python |             |                                         |
| /reference/pyspark.pandas/api/pyspark.p |             |                                         |
| andas.groupby.GroupBy.backfill.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `bfill <https                           | O           |                                         |
| ://spark.apache.org/docs/latest/api/pyt |             |                                         |
| hon/reference/pyspark.pandas/api/pyspar |             |                                         |
| k.pandas.groupby.GroupBy.bfill.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| boxplot                                 | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| corr                                    | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| corrwith                                | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `count <https                           | O           |                                         |
| ://spark.apache.org/docs/latest/api/pyt |             |                                         |
| hon/reference/pyspark.pandas/api/pyspar |             |                                         |
| k.pandas.groupby.GroupBy.count.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| cov                                     | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `cumcount <https://                     | O           |                                         |
| spark.apache.org/docs/latest/api/python |             |                                         |
| /reference/pyspark.pandas/api/pyspark.p |             |                                         |
| andas.groupby.GroupBy.cumcount.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `cummax <https:                         | O           |                                         |
| //spark.apache.org/docs/latest/api/pyth |             |                                         |
| on/reference/pyspark.pandas/api/pyspark |             |                                         |
| .pandas.groupby.GroupBy.cummax.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `cummin <https:                         | O           |                                         |
| //spark.apache.org/docs/latest/api/pyth |             |                                         |
| on/reference/pyspark.pandas/api/pyspark |             |                                         |
| .pandas.groupby.GroupBy.cummin.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `cumprod <https:/                       | O           |                                         |
| /spark.apache.org/docs/latest/api/pytho |             |                                         |
| n/reference/pyspark.pandas/api/pyspark. |             |                                         |
| pandas.groupby.GroupBy.cumprod.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `cumsum <https:                         | O           |                                         |
| //spark.apache.org/docs/latest/api/pyth |             |                                         |
| on/reference/pyspark.pandas/api/pyspark |             |                                         |
| .pandas.groupby.GroupBy.cumsum.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `describe <https://spark.apa            | O           | ``percentiles``, ``include``,           |
| che.org/docs/latest/api/python/referenc |             | ``exclude``, ``datetime_is_numeric``    |
| e/pyspark.pandas/api/pyspark.pandas.gro |             |                                         |
| upby.DataFrameGroupBy.describe.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `diff <http                             | O           | ``axis``                                |
| s://spark.apache.org/docs/latest/api/py |             |                                         |
| thon/reference/pyspark.pandas/api/pyspa |             |                                         |
| rk.pandas.groupby.GroupBy.diff.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| dtypes                                  | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| ewm                                     | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `ffill <https                           | O           |                                         |
| ://spark.apache.org/docs/latest/api/pyt |             |                                         |
| hon/reference/pyspark.pandas/api/pyspar |             |                                         |
| k.pandas.groupby.GroupBy.ffill.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `fillna <https:                         | O           | ``downcast``                            |
| //spark.apache.org/docs/latest/api/pyth |             |                                         |
| on/reference/pyspark.pandas/api/pyspark |             |                                         |
| .pandas.groupby.GroupBy.fillna.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `filter <https:                         | O           |                                         |
| //spark.apache.org/docs/latest/api/pyth |             |                                         |
| on/reference/pyspark.pandas/api/pyspark |             |                                         |
| .pandas.groupby.GroupBy.filter.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `first <https                           | O           | ``numeric_only``, ``min_count``         |
| ://spark.apache.org/docs/latest/api/pyt |             |                                         |
| hon/reference/pyspark.pandas/api/pyspar |             |                                         |
| k.pandas.groupby.GroupBy.first.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `get\_                                  | O           |                                         |
| group <https://s                        |             |                                         |
| park.apache.org/docs/latest/api/python/ |             |                                         |
| reference/pyspark.pandas/api/pyspark.pa |             |                                         |
| ndas.groupby.GroupBy.get_group.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| groups                                  | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `head <http                             | O           |                                         |
| s://spark.apache.org/docs/latest/api/py |             |                                         |
| thon/reference/pyspark.pandas/api/pyspa |             |                                         |
| rk.pandas.groupby.GroupBy.head.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| hist                                    | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `idxmax <https:                         | O           | ``axis``                                |
| //spark.apache.org/docs/latest/api/pyth |             |                                         |
| on/reference/pyspark.pandas/api/pyspark |             |                                         |
| .pandas.groupby.GroupBy.idxmax.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `idxmin <https:                         | O           | ``axis``                                |
| //spark.apache.org/docs/latest/api/pyth |             |                                         |
| on/reference/pyspark.pandas/api/pyspark |             |                                         |
| .pandas.groupby.GroupBy.idxmin.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| indices                                 | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `last <http                             | O           | ``numeric_only``, ``min_count``         |
| s://spark.apache.org/docs/latest/api/py |             |                                         |
| thon/reference/pyspark.pandas/api/pyspa |             |                                         |
| rk.pandas.groupby.GroupBy.last.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| mad                                     | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `max <htt                               | O           | ``numeric_only``, ``min_count``         |
| ps://spark.apache.org/docs/latest/api/p |             |                                         |
| ython/reference/pyspark.pandas/api/pysp |             |                                         |
| ark.pandas.groupby.GroupBy.max.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `mean <http                             | O           | ``numeric_only``, ``engine``            |
| s://spark.apache.org/docs/latest/api/py |             |                                         |
| thon/reference/pyspark.pandas/api/pyspa |             |                                         |
| rk.pandas.groupby.GroupBy.mean.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `median <https:                         | O           |                                         |
| //spark.apache.org/docs/latest/api/pyth |             |                                         |
| on/reference/pyspark.pandas/api/pyspark |             |                                         |
| .pandas.groupby.GroupBy.median.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `min <htt                               | O           |                                         |
| ps://spark.apache.org/docs/latest/api/p |             |                                         |
| ython/reference/pyspark.pandas/api/pysp |             |                                         |
| ark.pandas.groupby.GroupBy.min.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| ndim                                    | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| ngroup                                  | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| ngroups                                 | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| nth                                     | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `nunique <https:/                       | O           |                                         |
| /spark.apache.org/docs/latest/api/pytho |             |                                         |
| n/reference/pyspark.pandas/api/pyspark. |             |                                         |
| pandas.groupby.GroupBy.nunique.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| ohlc                                    | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| pad                                     | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| pct_c hange                             | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| pipe                                    | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| plot                                    | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| prod                                    | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| qua ntile                               | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `rank <http                             | O           | ``na_option``, ``pct``, ``axis``        |
| s://spark.apache.org/docs/latest/api/py |             |                                         |
| thon/reference/pyspark.pandas/api/pyspa |             |                                         |
| rk.pandas.groupby.GroupBy.rank.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| resample                                | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| sample                                  | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| sem                                     | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `shift <https                           | O           | ``freq``, ``axis``                      |
| ://spark.apache.org/docs/latest/api/pyt |             |                                         |
| hon/reference/pyspark.pandas/api/pyspar |             |                                         |
| k.pandas.groupby.GroupBy.shift.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `size <http                             | O           |                                         |
| s://spark.apache.org/docs/latest/api/py |             |                                         |
| thon/reference/pyspark.pandas/api/pyspa |             |                                         |
| rk.pandas.groupby.GroupBy.size.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| skew                                    | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `std <htt                               | O           | ``engine``                              |
| ps://spark.apache.org/docs/latest/api/p |             |                                         |
| ython/reference/pyspark.pandas/api/pysp |             |                                         |
| ark.pandas.groupby.GroupBy.std.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `sum <htt                               | O           | ``numeric_only``, ``min_count``         |
| ps://spark.apache.org/docs/latest/api/p |             |                                         |
| ython/reference/pyspark.pandas/api/pysp |             |                                         |
| ark.pandas.groupby.GroupBy.sum.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `tail <http                             | O           |                                         |
| s://spark.apache.org/docs/latest/api/py |             |                                         |
| thon/reference/pyspark.pandas/api/pyspa |             |                                         |
| rk.pandas.groupby.GroupBy.tail.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| take                                    | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `transform <https://s                   | O           | ``engine``                              |
| park.apache.org/docs/latest/api/python/ |             |                                         |
| reference/pyspark.pandas/api/pyspark.pa |             |                                         |
| ndas.groupby.GroupBy.transform.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| tshift                                  | X           |                                         |
+-----------------------------------------+-------------+-----------------------------------------+
| `var <htt                               | O           | ``engine``                              |
| ps://spark.apache.org/docs/latest/api/p |             |                                         |
| ython/reference/pyspark.pandas/api/pysp |             |                                         |
| ark.pandas.groupby.GroupBy.var.html>`__ |             |                                         |
+-----------------------------------------+-------------+-----------------------------------------+

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


=================================
Upgrading from PySpark 1.4 to 1.5
=================================

* Resolution of strings to columns in Python now supports using dots (.) to qualify the column or access nested values. For example ``df['table.column.nestedField']``. However, this means that if your column name contains any dots you must now escape them using backticks (e.g., ``table.`column.with.dots`.nested``).

* DataFrame.withColumn method in PySpark supports adding a new column or replacing existing columns of the same name.

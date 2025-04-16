---
layout: global
title: FOR statement
displayTitle: FOR statement
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

Repeat the execution of a list of statements for each row returned by query.

This statement may only be used within a [compound statement](compound-stmt.html).

## Syntax

```
[ label : ] FOR [ variable_name AS ] query
  DO
  { stmt ; } [...]
  END FOR [ label ]
```

## Parameters

- **label**

  An optional label for the loop which is unique amongst all labels for statements within which the `FOR` statement is contained.
  If an end label is specified, it must match the beginning label.
  The label can be used to [LEAVE](leave-stmt.html) or [ITERATE](iterate-stmt.html) the loop.
  To qualify loop column references, use the `variable_name`, not the `label`.

- **variable_name**

  An optional name you can use as a qualifier when referencing the columns in the cursor.

- **stmt**

  A SQL statement

## Notes

If the query operates on a table that is also modified within the loop's body, the semantics depend on the data source.
For Delta tables, the query will remain unaffected.
Spark does not guarantee the full execution of the query if the `FOR` loop completes prematurely due to a `LEAVE` statement or an error condition.
When exceptions or side-effects occur during the execution of the query, Spark does not guarantee at which point in time within the loop these occur.
Often `FOR` loops can be replaced with relational queries, which are typically more efficient.

## Examples

```SQL
-- sum up all odd numbers from 1 through 10
> BEGIN
    DECLARE sum INT DEFAULT 0;
    sumNumbers: FOR row AS SELECT num FROM range(1, 20) AS t(num) DO
      IF num > 10 THEN
         LEAVE sumNumbers;
      ELSEIF num % 2 = 0 THEN
        ITERATE sumNumbers;
      END IF;
      SET sum = sum + row.num;
    END FOR sumNumbers;
    VALUES (sum);
  END;
 25

-- Compare with the much more efficient relational computation:
> SELECT sum(num) FROM range(1, 10) AS t(num) WHERE num % 2 = 1;
 25
```

## Related articles

- [SQL Scripting](../sql-ref-scripting.html)
- [CASE Statement](../control-flow/case-stmt.html)
- [Compound Statement](../control-flow/compound-stmt.html)
- [LOOP Statement](../control-flow/loop-stmt.html)
- [WHILE Statement](../control-flow/while-stmt.html)
- [REPEAT Statement](../control-flow/repeat-stmt.html)
- [LEAVE Statement](../control-flow/leave-stmt.html)
- [ITERATE Statement](../control-flow/iterate-stmt.html)

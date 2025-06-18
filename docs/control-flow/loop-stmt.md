---
layout: global
title: LOOP statement
displayTitle: LOOP statement
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

Repeat the execution of a list of statements.

This statement may only be used within a [compound statement](compound-stmt.html).

## Syntax

```
[ label : ] LOOP
  { stmt ; } [...]
  END LOOP [ label ]
```

## Parameters

- **label**

  An optional label for the loop, which is unique amongst all labels for statements within which the `LOOP` statement is contained.
  If an end label is specified, it must match the beginning label.
  The label can be used to [LEAVE](leave-stmt.html) or [ITERATE](iterate-stmt.html) the loop.

- **stmt**

  A SQL statement

## Examples

```SQL
-- sum up all odd numbers from 1 through 10
> BEGIN
    DECLARE sum INT DEFAULT 0;
    DECLARE num INT DEFAULT 0;
    sumNumbers: LOOP
      SET num = num + 1;
      IF num > 10 THEN
        LEAVE sumNumbers;
      END IF;
      IF num % 2 = 0 THEN
        ITERATE sumNumbers;
      END IF;
      SET sum = sum + num;
    END LOOP sumNumbers;
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
- [FOR Statement](../control-flow/for-stmt.html)
- [WHILE Statement](../control-flow/while-stmt.html)
- [REPEAT Statement](../control-flow/repeat-stmt.html)
- [IF Statement](../control-flow/if-stmt.html)
- [ITERATE Statement](../control-flow/iterate-stmt.html)
- [LEAVE Statement](../control-flow/leave-stmt.html)


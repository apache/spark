---
layout: global
title: WHILE statement
displayTitle: WHILE statement
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

# WHILE statement

Repeat the execution of a list of statements while a condition is true.

This statement may only be used within a [compound statement](compound-stmt.md).

## Syntax

```
[ label : ] WHILE cond DO
  { stmt ; } [...]
  END WHILE [ label ]
```

## Parameters

- **[label](/sql/language-manual/sql-ref-names.md#label-name)**

  An optional label for the loop, which is unique amongst all labels for statements within which the `WHILE` statement is contained.
  The label can be used to [LEAVE](leave-stmt.md) or [ITERATE](iterate-stmt.md) the loop.

- **`cond`**

  Any expression evaluating to a `BOOLEAN`

- **`stmt`**

  A SQL statement

## Examples

```SQL
-- sum up all odd numbers from 1 through 10
> BEGIN
    DECLARE sum INT DEFAULT 0;
    DECLARE num INT DEFAULT 0;
    sumNumbers: WHILE num < 10 DO
      SET num = num + 1;
      IF num % 2 = 0 THEN
        ITERATE sumNumbers;
      END IF;
      SET sum = sum + num;
    END WHILE sumNumbers;
    VALUES (sum);
  END;
 25

-- Compare with the much more efficient relational computation:
> SELECT sum(num) FROM range(1, 10) AS t(num) WHERE num % 2 = 1;
 25
```

## Related articles

- [SQL Scripting](/sql/language-manual/sql-ref-scripting.md)
- [CASE Statement](/sql/language-manual/control-flow/case-stmt.md)
- [Compound Statement](/sql/language-manual/control-flow/compound-stmt.md)
- [FOR Statement](/sql/language-manual/control-flow/for-stmt.md)
- [REPEAT Statement](/sql/language-manual/control-flow/repeat-stmt.md)
- [IF Statement](/sql/language-manual/control-flow/if-stmt.md)
- [ITERATE Statement](/sql/language-manual/control-flow/iterate-stmt.md)
- [LEAVE Statement](/sql/language-manual/control-flow/leave-stmt.md)
- [LOOP Statement](/sql/language-manual/control-flow/loop-stmt.md)

```

```

---
layout: global
title: LEAVE statement
displayTitle: LEAVE statement
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

# LEAVE statement

Terminates the execution of an iteration of a looping statement and continues with the next iteration if the looping condition is met.

This statement may only be used within a [compound statement](compound-stmt.md).

## Syntax

```
ITERATE label
```

## Parameters

- **[label](/sql/language-manual/sql-ref-names.md#label-name)**

  The label identifies a statement to leave that directly or indirectly contains the `LEAVE` statement.

## Examples

```SQL
-- sum up all odd numbers from 1 through 10
-- Iterate over even numbers and leave the loop after 10 has been reached.
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
```

## Related articles

- [SQL Scripting](/sql/language-manual/sql-ref-scripting.md)
- [CASE Statement](/sql/language-manual/control-flow/case-stmt.md)
- [Compound Statement](/sql/language-manual/control-flow/compound-stmt.md)
- [FOR Statement](/sql/language-manual/control-flow/for-stmt.md)
- [LOOP Statement](/sql/language-manual/control-flow/loop-stmt.md)
- [WHILE Statement](/sql/language-manual/control-flow/while-stmt.md)
- [IF Statement](/sql/language-manual/control-flow/if-stmt.md)
- [ITERATE Statement](/sql/language-manual/control-flow/iterate-stmt.md)

```

```

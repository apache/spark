---
layout: global
title: IF statement
displayTitle: IF statement
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

Executes lists of statements based on the first condition that evaluates to true.

This statement may only be used within a [compound statement](compound-stmt.html).

## Syntax

```
IF condition  THEN { stmt ; } [...]
  [ { ELSEIF condition THEN { stmt ; } [...] } [...] ]
  [ ELSE { stmt ; } [...] ]
  END IF
```

## Parameters

- **condition**

  Any expression evaluating to a BOOLEAN.

- **stmt**

  A SQL statement to execute if the `condition` is `true`.

## Examples

```SQL
> BEGIN
    DECLARE choice DOUBLE DEFAULT 3.9;
    DECLARE result STRING;
    IF choice < 2 THEN
      VALUES ('one fish');
    ELSEIF choice < 3 THEN
      VALUES ('two fish');
    ELSEIF choice < 4 THEN
      VALUES ('red fish');
    ELSEIF choice < 5 OR choice IS NULL THEN
      VALUES ('blue fish');
    ELSE
      VALUES ('no fish');
    END IF;
  END;
 red fish
```

## Related articles

- [SQL Scripting](../sql-ref-scripting.html)
- [CASE Statement](../control-flow/case-stmt.html)
- [Compound Statement](../control-flow/compound-stmt.html)
- [FOR Statement](../control-flow/for-stmt.html)

---
layout: global
title: RESIGNAL statement
displayTitle: RESIGNAL statement
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

Re-raises the condition handled by the condition handler.

This statement may only be used within a [compound statement](compound-stmt.html).

## Syntax

```
RESIGNAL
```

## Parameters

None

## Examples

```SQL
> CREATE TABLE log(eventtime TIMESTAMP, log STRING);

> BEGIN
    DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
      BEGIN
        DECLARE cond STRING;
        DECLARE message STRING;
        DECLARE state STRING;
        DECLARE args MAP<STRING, STRING>;
        DECLARE line BIGINT;
        DECLARE argstr STRING;
        DECLARE log STRING;
        GET DIAGNOSTICS CONDITION 1
           cond = CONDITION_IDENTIFIER,
           message = MESSAGE_TEXT,
           state = RETURNED_SQLSTATE,
           args = MESSAGE_ARGUMENTS,
           line = LINE_NUMBER;
        SET argstr =
          (SELECT aggregate(array_agg('Parm:' || key || ' Val: value '),
                            '', (acc, x)->(acc || ' ' || x))
             FROM explode(args) AS args(key, val));
        SET log = 'Condition: ' || cond ||
                  ' Message: ' || message ||
                  ' SQLSTATE: ' || state ||
                  ' Args: ' || argstr ||
                  ' Line: ' || line;
        INSERT INTO log VALUES(current_timestamp(), log);
        RESIGNAL;
      END;
    SELECT 10/0;
  END;
 [DIVIDE_BY_ZERO] Division by zero. Use try_divide to tolerate divisor being 0 and return NULL instead.

> SELECT * FROM log ORDER BY eventtime DESC LIMIT 1;
 Condition: DIVIDE_BY_ZERO Message: Division by zero. Use try_divide to tolerate divisor being 0 and return NULL instead. SQLSTATE: 22012 Args: Line: 28
```

## Related articles

- [SQL Scripting](../sql-ref-scripting.html)
- [CASE Statement](../control-flow/case-stmt.html)
- [Compound Statement](../control-flow/compound-stmt.html)
- [SIGNAL Statement](../control-flow/signal-stmt.html)
- [FOR Statement](../control-flow/for-stmt.html)
- [IF Statement](../control-flow/if-stmt.html)
- [ITERATE Statement](../control-flow/iterate-stmt.html)
- [REPEAT Statement](../control-flow/repeat-stmt.html)
- [SIGNAL Statement](../control-flow/signal-stmt.html)
- [Error handling and error messages](/error-messages/index.html)

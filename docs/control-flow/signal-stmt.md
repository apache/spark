---
layout: global
title: SIGNAL statement
displayTitle: SIGNAL statement
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

Raises a condition.

This statement may only be used within a [compound statement](compound-stmt.html).

Note: Spark recommends using [RESIGNAL](resignal-stmt.html) to raise conditions from within a handler.
RESIGNAL builds a diagnostic stack in the SQL Standard, while `SIGNAL` clears the stack.
Using `RESIGNAL` within a handler preserves future exploitation of the diagnostic stack.

## Syntax

```
SIGNAL { condition_name
         [ SET { MESSAGE_ARGUMENTS = argument_map |
                 MESSAGE_TEXT = message_str } ] |
         SQLSTATE [VALUE] sqlstate [ SET MESSAGE_TEXT = message_str ] }
```

## Parameters

- **condition_name**

  The name of a locally defined condition or system-defined error condition.

- **argument_map**

  Optionally, a `MAP<STRING, STRING>` literal that assigns values to a system-defined parameterized condition message.

- **message_str**

  Optionally, a `STRING` literal that provides a message string to the raised `SQLSTATE` or user-defined condition.

- **sqlstate**

  A `STRING` literal of length 5. If specified, raise `USER_RAISED_EXCEPTION` with the specified `SQLSTATE`.

## Examples

```SQL
> DECLARE input INT DEFAULT 5;

> BEGIN
    DECLARE arg_map MAP<STRING, STRING>;
    IF input > 4 THEN
      SET arg_map = map('errorMessage',
                        'Input must be <= 4.');
      SIGNAL USER_RAISED_EXCEPTION
        SET MESSAGE_ARGUMENTS = arg_map;
    END IF;
  END;
```

## Related articles

- [SQL Scripting](../sql-ref-scripting.html)
- [CASE Statement](../control-flow/case-stmt.html)
- [Compound Statement](../control-flow/compound-stmt.html)
- [FOR Statement](../control-flow/for-stmt.html)
- [IF Statement](../control-flow/if-stmt.html)
- [ITERATE Statement](../control-flow/iterate-stmt.html)
- [REPEAT Statement](../control-flow/repeat-stmt.html)
- [RESIGNAL Statement](../control-flow/resignal-stmt.html)
- [Error handling and error messages](/error-messages/index.html)

---
layout: global
title: SET TIME ZONE
displayTitle: SET TIME ZONE
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

### Description

The SET TIME ZONE command sets the time zone of the current session.

### Syntax

```sql
SET TIME ZONE LOCAL
SET TIME ZONE 'timezone_value'
SET TIME ZONE INTERVAL interval_literal
```

### Parameters

* **LOCAL**

    Set the time zone to the one specified in the java `user.timezone` property, or to the environment variable `TZ` if `user.timezone` is undefined, or to the system time zone if both of them are undefined.

* **timezone_value**

    The ID of session local timezone in the format of either region-based zone IDs or zone offsets. Region IDs must have the form 'area/city', such as 'America/Los_Angeles'. Zone offsets must be in the format '`(+|-)HH`', '`(+|-)HH:mm`' or '`(+|-)HH:mm:ss`', e.g '-08', '+01:00' or '-13:33:33'. Also, 'UTC' and 'Z' are supported as aliases of '+00:00'. Other short names are not recommended to use because they can be ambiguous.

* **interval_literal**

    The [interval literal](sql-ref-literals.html#interval-literal) represents the difference between the session time zone to the 'UTC'. It must be in the range of [-18, 18] hours and max to second precision, e.g. `INTERVAL 2 HOURS 30 MINUTES` or `INTERVAL '15:40:32' HOUR TO SECOND`.

### Examples

```sql
-- Set time zone to the system default.
SET TIME ZONE LOCAL;

-- Set time zone to the region-based zone ID.
SET TIME ZONE 'America/Los_Angeles';

-- Set time zone to the Zone offset.
SET TIME ZONE '+08:00';

-- Set time zone with intervals.
SET TIME ZONE INTERVAL 1 HOUR 30 MINUTES;
SET TIME ZONE INTERVAL '08:30:00' HOUR TO SECOND;
```

### Related Statements

* [SET](sql-ref-syntax-aux-conf-mgmt-set.html)

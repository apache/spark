---
layout: global
title: Datetime patterns
displayTitle: Datetime Patterns for Formatting and Parsing
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

There are several common scenarios for datetime usage in Spark:

- CSV/JSON datasources use the pattern string for parsing and formatting datetime content.

- Datetime functions related to convert `StringType` to/from `DateType` or `TimestampType`.
  For example, `unix_timestamp`, `date_format`, `to_unix_timestamp`, `from_unixtime`, `to_date`, `to_timestamp`, `from_utc_timestamp`, `to_utc_timestamp`, etc.

Spark uses pattern letters in the following table for date and timestamp parsing and formatting:

|Symbol|Meaning|Presentation|Examples|
|--- |--- |--- |--- |
|**G**|era|text|AD; Anno Domini; A|
|**y**|year|year|2020; 20|
|**D**|day-of-year|number|189|
|**M/L**|month-of-year|number/text|7; 07; Jul; July; J|
|**d**|day-of-month|number|28|
|**Q/q**|quarter-of-year|number/text|3; 03; Q3; 3rd quarter|
|**Y**|week-based-year|year|1996; 96|
|**w**|week-of-week-based-year|number|27|
|**W**|week-of-month|number|4|
|**E**|day-of-week|text|Tue; Tuesday; T|
|**u**|localized day-of-week|number/text|2; 02; Tue; Tuesday; T|
|**F**|week-of-month|number|3|
|**a**|am-pm-of-day|text|PM|
|**h**|clock-hour-of-am-pm (1-12)|number|12|
|**K**|hour-of-am-pm (0-11)|number|0|
|**k**|clock-hour-of-day (1-24)|number|0|
|**H**|hour-of-day (0-23)|number|0|
|**m**|minute-of-hour|number|30|
|**s**|second-of-minute|number|55|
|**S**|fraction-of-second|fraction|978|
|**V**|time-zone ID|zone-id|America/Los_Angeles; Z; -08:30|
|**z**|time-zone name|zone-name|Pacific Standard Time; PST|
|**O**|localized zone-offset|offset-O|GMT+8; GMT+08:00; UTC-08:00;|
|**X**|zone-offset 'Z' for zero|offset-X|Z; -08; -0830; -08:30; -083015; -08:30:15;|
|**x**|zone-offset|offset-x|+0000; -08; -0830; -08:30; -083015; -08:30:15;|
|**Z**|zone-offset|offset-Z|+0000; -0800; -08:00;|
|**'**|escape for text|delimiter||
|**''**|single quote|literal|'|
|**[**|optional section start|||
|**]**|optional section end|||

The count of pattern letters determines the format.

- Text: The text style is determined based on the number of pattern letters used. Less than 4 pattern letters will use the short form. Exactly 4 pattern letters will use the full form. Exactly 5 pattern letters will use the narrow form. Six or more letters will fail.

- Number: If the count of letters is one, then the value is output using the minimum number of digits and without padding. Otherwise, the count of digits is used as the width of the output field, with the value zero-padded as necessary. The following pattern letters have constraints on the count of letters. Only one letter 'F' can be specified. Up to two letters of 'd', 'H', 'h', 'K', 'k', 'm', and 's' can be specified. Up to three letters of 'D' can be specified.

- Number/Text: If the count of pattern letters is 3 or greater, use the Text rules above. Otherwise use the Number rules above.

- Fraction: Use one or more (up to 9) contiguous `'S'` characters, e,g `SSSSSS`, to parse and format fraction of second.
  For parsing, the acceptable fraction length can be [1, the number of contiguous 'S'].
  For formatting, the fraction length would be padded to the number of contiguous 'S' with zeros.
  Spark supports datetime of micro-of-second precision, which has up to 6 significant digits, but can parse nano-of-second with exceeded part truncated.

- Year: The count of letters determines the minimum field width below which padding is used. If the count of letters is two, then a reduced two digit form is used. For printing, this outputs the rightmost two digits. For parsing, this will parse using the base value of 2000, resulting in a year within the range 2000 to 2099 inclusive. If the count of letters is less than four (but not two), then the sign is only output for negative years. Otherwise, the sign is output if the pad width is exceeded when 'G' is not present.

- Zone ID(V): This outputs the display the time-zone ID. Pattern letter count must be 2.

- Zone names(z): This outputs the display textual name of the time-zone ID. If the count of letters is one, two or three, then the short name is output. If the count of letters is four, then the full name is output. Five or more letters will fail.

- Offset X and x: This formats the offset based on the number of pattern letters. One letter outputs just the hour, such as '+01', unless the minute is non-zero in which case the minute is also output, such as '+0130'. Two letters outputs the hour and minute, without a colon, such as '+0130'. Three letters outputs the hour and minute, with a colon, such as '+01:30'. Four letters outputs the hour and minute and optional second, without a colon, such as '+013015'. Five letters outputs the hour and minute and optional second, with a colon, such as '+01:30:15'. Six or more letters will fail. Pattern letter 'X' (upper case) will output 'Z' when the offset to be output would be zero, whereas pattern letter 'x' (lower case) will output '+00', '+0000', or '+00:00'.

- Offset O: This formats the localized offset based on the number of pattern letters. One letter outputs the short form of the localized offset, which is localized offset text, such as 'GMT', with hour without leading zero, optional 2-digit minute and second if non-zero, and colon, for example 'GMT+8'. Four letters outputs the full form, which is localized offset text, such as 'GMT, with 2-digit hour and minute field, optional second field if non-zero, and colon, for example 'GMT+08:00'. Any other count of letters will fail.

- Offset Z: This formats the offset based on the number of pattern letters. One, two or three letters outputs the hour and minute, without a colon, such as '+0130'. The output will be '+0000' when the offset is zero. Four letters outputs the full form of localized offset, equivalent to four letters of Offset-O. The output will be the corresponding localized offset text if the offset is zero. Five letters outputs the hour, minute, with optional second if non-zero, with colon. It outputs 'Z' if the offset is zero. Six or more letters will fail.

- Optional section start and end: Use `[]` to define an optional section and maybe nested.
  During formatting, all valid data will be output even it is in the optional section.
  During parsing, the whole section may be missing from the parsed string.
  An optional section is started by `[` and ended using `]` (or at the end of the pattern).

More details for the text style:

- Short Form: Short text, typically an abbreviation. For example, day-of-week Monday might output "Mon".

- Full Form: Full text, typically the full description. For example, day-of-week Monday might output "Monday".

- Narrow Form: Narrow text, typically a single letter. For example, day-of-week Monday might output "M".

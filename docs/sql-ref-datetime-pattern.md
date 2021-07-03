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
|------|-------|------------|--------|
|**G**|era|text|AD; Anno Domini|
|**y**|year|year|2020; 20|
|**D**|day-of-year|number(3)|189|
|**M/L**|month-of-year|month|7; 07; Jul; July|
|**d**|day-of-month|number(3)|28|
|**Q/q**|quarter-of-year|number/text|3; 03; Q3; 3rd quarter|
|**E**|day-of-week|text|Tue; Tuesday|
|**F**|aligned day of week in month|number(1)|3|
|**a**|am-pm-of-day|am-pm|PM|
|**h**|clock-hour-of-am-pm (1-12)|number(2)|12|
|**K**|hour-of-am-pm (0-11)|number(2)|0|
|**k**|clock-hour-of-day (1-24)|number(2)|0|
|**H**|hour-of-day (0-23)|number(2)|0|
|**m**|minute-of-hour|number(2)|30|
|**s**|second-of-minute|number(2)|55|
|**S**|fraction-of-second|fraction|978|
|**V**|time-zone ID|zone-id|America/Los_Angeles; Z; -08:30|
|**z**|time-zone name|zone-name|Pacific Standard Time; PST|
|**O**|localized zone-offset|offset-O|GMT+8; GMT+08:00; UTC-08:00;|
|**X**|zone-offset 'Z' for zero|offset-X|Z; -08; -0830; -08:30; -083015; -08:30:15;|
|**x**|zone-offset|offset-x|+0000; -08; -0830; -08:30; -083015; -08:30:15;|
|**Z**|zone-offset|offset-Z|+0000; -0800; -08:00;|
|**'**|escape for text|delimiter| |
|**''**|single quote|literal|'|
|**[**|optional section start| | |
|**]**|optional section end| | |

The count of pattern letters determines the format.

- Text: The text style is determined based on the number of pattern letters used. Less than 4 pattern letters will use the short text form, typically an abbreviation, e.g. day-of-week Monday might output "Mon". Exactly 4 pattern letters will use the full text form, typically the full description, e.g, day-of-week Monday might output "Monday". 5 or more letters will fail.

- Number(n): The n here represents the maximum count of letters this type of datetime pattern can be used. If the count of letters is one, then the value is output using the minimum number of digits and without padding. Otherwise, the count of digits is used as the width of the output field, with the value zero-padded as necessary.

- Number/Text: If the count of pattern letters is 3 or greater, use the Text rules above. Otherwise use the Number rules above.

- Fraction: Use one or more (up to 9) contiguous `'S'` characters, e,g `SSSSSS`, to parse and format fraction of second.
  For parsing, the acceptable fraction length can be [1, the number of contiguous 'S'].
  For formatting, the fraction length would be padded to the number of contiguous 'S' with zeros.
  Spark supports datetime of micro-of-second precision, which has up to 6 significant digits, but can parse nano-of-second with exceeded part truncated.

- Year: The count of letters determines the minimum field width below which padding is used. If the count of letters is two, then a reduced two digit form is used. For printing, this outputs the rightmost two digits. For parsing, this will parse using the base value of 2000, resulting in a year within the range 2000 to 2099 inclusive. If the count of letters is less than four (but not two), then the sign is only output for negative years. Otherwise, the sign is output if the pad width is exceeded when 'G' is not present. 7 or more letters will fail.

- Month: It follows the rule of Number/Text. The text form is depend on letters - 'M' denotes the 'standard' form, and 'L' is for 'stand-alone' form. These two forms are different only in some certain languages. For example, in Russian, 'Июль' is the stand-alone form of July, and 'Июля' is the standard form. Here are examples for all supported pattern letters:
  - `'M'` or `'L'`: Month number in a year starting from 1. There is no difference between 'M' and 'L'. Month from 1 to 9 are printed without padding.
    ```sql
    spark-sql> select date_format(date '1970-01-01', "M");
    1
    spark-sql> select date_format(date '1970-12-01', "L");
    12
    ```
  - `'MM'` or `'LL'`: Month number in a year starting from 1. Zero padding is added for month 1-9.
      ```sql
      spark-sql> select date_format(date '1970-1-01', "LL");
      01
      spark-sql> select date_format(date '1970-09-01', "MM");
      09
      ```
  - `'MMM'`: Short textual representation in the standard form. The month pattern should be a part of a date pattern not just a stand-alone month except locales where there is no difference between stand and stand-alone forms like in English.
    ```sql
    spark-sql> select date_format(date '1970-01-01', "d MMM");
    1 Jan
    spark-sql> select to_csv(named_struct('date', date '1970-01-01'), map('dateFormat', 'dd MMM', 'locale', 'RU'));
    01 янв.
    ```
  - `'LLL'`: Short textual representation in the stand-alone form. It should be used to format/parse only months without any other date fields.
    ```sql
    spark-sql> select date_format(date '1970-01-01', "LLL");
    Jan
    spark-sql> select to_csv(named_struct('date', date '1970-01-01'), map('dateFormat', 'LLL', 'locale', 'RU'));
    янв.
    ```
  - `'MMMM'`: full textual month representation in the standard form. It is used for parsing/formatting months as a part of dates/timestamps.
    ```sql
    spark-sql> select date_format(date '1970-01-01', "d MMMM");
    1 January
    spark-sql> select to_csv(named_struct('date', date '1970-01-01'), map('dateFormat', 'd MMMM', 'locale', 'RU'));
    1 января
    ```
  - `'LLLL'`: full textual month representation in the stand-alone form. The pattern can be used to format/parse only months.
    ```sql
    spark-sql> select date_format(date '1970-01-01', "LLLL");
    January
    spark-sql> select to_csv(named_struct('date', date '1970-01-01'), map('dateFormat', 'LLLL', 'locale', 'RU'));
    январь
    ```

- am-pm: This outputs the am-pm-of-day. Pattern letter count must be 1.

- Zone ID(V): This outputs the display the time-zone ID. Pattern letter count must be 2.

- Zone names(z): This outputs the display textual name of the time-zone ID. If the count of letters is one, two or three, then the short name is output. If the count of letters is four, then the full name is output. Five or more letters will fail.

- Offset X and x: This formats the offset based on the number of pattern letters. One letter outputs just the hour, such as '+01', unless the minute is non-zero in which case the minute is also output, such as '+0130'. Two letters outputs the hour and minute, without a colon, such as '+0130'. Three letters outputs the hour and minute, with a colon, such as '+01:30'. Four letters outputs the hour and minute and optional second, without a colon, such as '+013015'. Five letters outputs the hour and minute and optional second, with a colon, such as '+01:30:15'. Six or more letters will fail. Pattern letter 'X' (upper case) will output 'Z' when the offset to be output would be zero, whereas pattern letter 'x' (lower case) will output '+00', '+0000', or '+00:00'.

- Offset O: This formats the localized offset based on the number of pattern letters. One letter outputs the short form of the localized offset, which is localized offset text, such as 'GMT', with hour without leading zero, optional 2-digit minute and second if non-zero, and colon, for example 'GMT+8'. Four letters outputs the full form, which is localized offset text, such as 'GMT, with 2-digit hour and minute field, optional second field if non-zero, and colon, for example 'GMT+08:00'. Any other count of letters will fail.

- Offset Z: This formats the offset based on the number of pattern letters. One, two or three letters outputs the hour and minute, without a colon, such as '+0130'. The output will be '+0000' when the offset is zero. Four letters outputs the full form of localized offset, equivalent to four letters of Offset-O. The output will be the corresponding localized offset text if the offset is zero. Five letters outputs the hour, minute, with optional second if non-zero, with colon. It outputs 'Z' if the offset is zero. Six or more letters will fail.

- Optional section start and end: Use `[]` to define an optional section and maybe nested.
  During formatting, all valid data will be output even it is in the optional section.
  During parsing, the whole section may be missing from the parsed string.
  An optional section is started by `[` and ended using `]` (or at the end of the pattern).
  
- Symbols of 'E', 'F', 'q' and 'Q' can only be used for datetime formatting, e.g. `date_format`. They are not allowed used for datetime parsing, e.g. `to_timestamp`.

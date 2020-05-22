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

The following tables define how the pattern letters be used for date and timestamp parsing and formatting in Spark.

## Date Fields

Pattern letters to output a date:

|Pattern|Count|Meaning|Presentation|Examples|
|---|---|---|---|---|
|**G**|1|era|text|AD|
|**GG**|2|era|text|AD|
|**GGG**|3|era|text|AD|
|**GGGG**|4|era|text|Anno Domini|
|**y**|1|year|year|2020|
|**yy**|2|year|year|20|
|**yyy**|3|year|year|2020|
|**y..y**|4..n|year|year|2020; 02020|
|**Y**|1|week-based-year|year|1996|
|**YY**|2|week-based-year|year|96|
|**YYY**|3|week-based-year|year|1996|
|**Y..Y**|4..n|week-based-year|year|1996; 01996|
|**Q**|1|quarter-of-year|number/text|3|
|**QQ**|2|quarter-of-year|number/text|03|
|**QQQ**|3|quarter-of-year|number/text|Q3|
|**QQQQ**|4|quarter-of-year|number/text|3rd quarter|
|**M**|1|month-of-year|number/text|7|
|**MM**|2|month-of-year|number/text|07|
|**MMM**|3|month-of-year|number/text|Jul|
|**MMMM**|4|month-of-year|number/text|July|
|**L**|1|month-of-year|number/text|7|
|**LL**|2|month-of-year|number/text|07|
|**LLL**|3|month-of-year|number/text|Jul|
|**LLLL**|4|month-of-year|number/text|July|
|**w**|1|week-of-week-based-year|number|1; 27|
|**ww**|1|week-of-week-based-year|number|01; 27|
|**W**|1|week-of-month|number|4|
|**D**|1|day-of-year|number|1; 189|
|**DD**|1|day-of-year|number|01; 189|
|**DDD**|1|day-of-year|number|001; 189|
|**d**|1|day-of-month|number|1; 28|
|**dd**|1|day-of-month|number|01; 28|
|**E**|1|day-of-week|text|Tue|
|**EE**|2|day-of-week|text|Tue|
|**EEE**|3|day-of-week|text|Tue|
|**EEEE**|4|day-of-week|text|Tuesday|
|**u**|1|localized day-of-week|number/text|2|
|**uu**|2|localized day-of-week|number/text|02|
|**uuu**|3|localized day-of-week|number/text|Tue|
|**uuuu**|4|localized day-of-week|number/text|Tuesday|
|**F**|1|week-of-month|number|3|

## Time Fields

Pattern letters to output a time:

|Pattern|Count|Meaning|Presentation|Examples|
|---|---|---|---|---|
|**a**|1|am-pm-of-day|text|PM|
|**h**|1|clock-hour-of-am-pm (1-12)|number|1; 12|
|**hh**|1|clock-hour-of-am-pm (1-12)|number|01; 12|
|**K**|1|hour-of-am-pm (0-11)|number|1; 11|
|**KK**|2|hour-of-am-pm (0-11)|number|01; 11|
|**k**|1|clock-hour-of-day (1-24)|number|1; 23|
|**kk**|2|clock-hour-of-day (1-24)|number|01; 23|
|**H**|1|hour-of-day (0-23)|number|1; 23|
|**HH**|1|hour-of-day (0-23)|number|01; 23|
|**m**|1|minute-of-hour|number|1; 30|
|**mm**|2|minute-of-hour|number|01; 30|
|**s**|1|second-of-minute|number|55|
|**ss**|2|second-of-minute|number|55|
|**S**|1..9|fraction-of-second|fraction|978|

## Zone ID

Pattern letters to output Zone Id:

|Pattern|Count|Meaning|Presentation|Examples|
|---|---|---|---|---|
|**VV**|2|time-zone ID|zone-id|America/Los_Angeles; Z; -08:30|
|**z**|1|time-zone name|zone-name|PST|
|**zz**|2|time-zone name|zone-name|PST|
|**zzz**|3|time-zone name|zone-name|PST|
|**zzzz**|4|time-zone name|zone-name|Pacific Standard Time|

## Zone offset

Pattern letters to output Zone Offset:

|Pattern|Count|Meaning|Presentation|Examples|
|---|---|---|---|---|
|**O**|1|localized zone-offset|offset-O|GMT+8|
|**OOOO**|4|localized zone-offset|offset-O|GMT+08:00|
|**X**|1|zone-offset 'Z' for zero|offset-X|Z; -08|
|**XX**|2|zone-offset 'Z' for zero|offset-X|Z; -0830|
|**XXX**|3|zone-offset 'Z' for zero|offset-X|Z; -08:30|
|**XXXX**|4|zone-offset 'Z' for zero|offset-X|Z; -083015|
|**XXXXX**|5|zone-offset 'Z' for zero|offset-X|Z; -08:30:15|
|**x**|1|zone-offset|offset-x|-08|
|**xx**|2|zone-offset|offset-x|-0830|
|**xxx**|3|zone-offset|offset-x|-08:30|
|**xxxx**|4|zone-offset|offset-x|-083015|
|**xxxxx**|5|zone-offset|offset-x|-08:30:15|
|**Z**|1|zone-offset|offset-Z|-0800|
|**ZZ**|2|zone-offset|offset-Z|-0800|
|**ZZZ**|3|zone-offset|offset-Z|-0800|
|**ZZZZ**|4|zone-offset|offset-Z|GMT-08:00|
|**ZZZZZ**|5|zone-offset|offset-Z|-08:00|

## Modifiers

Pattern letters that modify the rest of the pattern:

|Pattern|Count|Meaning|Presentation|Examples|
|---|---|---|---|---|
|**'**|1|escape for text|delimiter| |
|**''**|1|single quote|literal|'|
|**[**|1|optional section start| | |
|**]**|1|optional section end| | |


- Count: The count of pattern letters determines the format. `1..n` describes the range how many numbers the pattern can have up to, `n` means no limit.

- Text: The text style is determined based on the number of pattern letters used. Less than 4 pattern letters will use the short form. Exactly 4 pattern letters will use the full form. 5 or more letters will fail.

- Number: If the count of letters is one, then the value is output using the minimum number of digits and without padding. Otherwise, the count of digits is used as the width of the output field, with the value zero-padded as necessary. The following pattern letters have constraints on the count of letters. Only one letter 'F' can be specified. Up to two letters of 'd', 'H', 'h', 'K', 'k', 'm', and 's' can be specified. Up to three letters of 'D' can be specified.

- Number/Text: If the count of pattern letters is 3 or greater, use the Text rules above. Otherwise use the Number rules above.

- Fraction: Use one or more (up to 9) contiguous `'S'` characters, e,g `SSSSSS`, to parse and format fraction of second.
  For parsing, the acceptable fraction length can be [1, the number of contiguous 'S'].
  For formatting, the fraction length would be padded to the number of contiguous 'S' with zeros.
  Spark supports datetime of micro-of-second precision, which has up to 6 significant digits, but can parse nano-of-second with exceeded part truncated.

- Year: The count of letters determines the minimum field width below which padding is used. If the count of letters is two, then a reduced two digit form is used. For printing, this outputs the rightmost two digits. For parsing, this will parse using the base value of 2000, resulting in a year within the range 2000 to 2099 inclusive. If the count of letters is less than four (but not two), then the sign is only output for negative years. Otherwise, the sign is output if the pad width is exceeded when 'G' is not present.

- Month: If the number of pattern letters is 3 or more, the month is interpreted as text; otherwise, it is interpreted as a number. The text form is depend on letters - 'M' denotes the 'standard' form, and 'L' is for 'stand-alone' form. The difference between the 'standard' and 'stand-alone' forms is trickier to describe as there is no difference in English. However, in other languages there is a difference in the word used when the text is used alone, as opposed to in a complete date. For example, the word used for a month when used alone in a date picker is different to the word used for month in association with a day and year in a date. In Russian, 'Июль' is the stand-alone form of July, and 'Июля' is the standard form. Here are examples for all supported pattern letters (more than 4 letters is invalid):
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
    spark-sql> select date_format(date '1970-01-01', "MMMM yyyy");
    January 1970
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

- AM/PM(a): This outputs the am-pm-of-day. Pattern letter count must be 1.

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

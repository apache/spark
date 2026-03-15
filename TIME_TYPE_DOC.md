# TIME Data Type in Spark SQL

This document describes the implementation and usage of the `TIME` data type in Spark SQL.

## Overview

The `TIME` data type represents a time of day, independent of any date or time zone. It is stored internally as a `Long` value representing the number of microseconds since midnight.

- **Internal Representation**: `Long` (microseconds since midnight)
- **Valid Range**: `00:00:00.000000` to `23:59:59.999999` (0 to 86,399,999,999 microseconds)
- **Precision**: Microsecond

## Usage

### SQL
```sql
CREATE TABLE events (id INT, event_time TIME);
INSERT INTO events VALUES (1, '10:30:00'), (2, '14:00:00.123456');
SELECT * FROM events WHERE event_time > '12:00:00';
```

### DataFrame API
```scala
import java.time.LocalTime
import org.apache.spark.sql.types.TimeType

val data = Seq(LocalTime.of(10, 30), LocalTime.of(14, 0))
val df = data.map(Tuple1(_)).toDF("t")
df.write.parquet("path/to/data")
```

## Data Source Support

- **Parquet**: Stored as `INT64` with the `time` logical type and `MICROS` unit.
- **ORC**: Stored as `LONG` with a `time` attribute.
- **JSON/CSV**: Stored as a string in `HH:mm:ss.SSSSSS` format by default. Supports custom formats via `timeFormat` option.
- **Hive**: Integrated with Hive via Spark-side schema serialization, as Hive does not natively support a `TIME` type.

## Implementation Details

- **TimeUtils**: Core utility class for time manipulation and parsing.
- **TimeType**: Definition of the data type in Spark's type system.
- **TimeCast**: Logic for casting to and from the `TIME` type.
- **TimeFormatter**: Specialized formatter for `TIME` values in text-based data sources.
- **Catalyst Expressions**: Support for `TIME` in various expressions, including aggregations and partitioning.

## Performance

- Highly efficient long-based internal representation.
- Optimized codegen for string conversion and comparison.
- Binary formats like Parquet provide native support and optimal performance.

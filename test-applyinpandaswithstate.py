import calendar
import os
import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row

def user_func(key, pdf, state):
  timeout_delay_sec = 10

  print('=' * 80)
  print(key)
  print(pdf)
  print(state.getOption)
  print(state.hasTimedOut)
  print('=' * 80)

  if state.hasTimedOut:
    state.remove()
    return pd.DataFrame({'key1': [], 'key2': [], 'maxTimestampSeenMs': [], 'average': []})
  else:
    prev_state = state.getOption
    if prev_state is None:
      prev_sum = 0
      prev_count = 0
      prev_max_timestamp_seen_sec = 0 # should be -Inf or something along with
    else:
      # FIXME: Is it better UX to access the state object as tuple instead of Row or dict at least?
      prev_sum = prev_state[0]
      prev_count = prev_state[1]
      prev_max_timestamp_seen_sec = prev_state[2]

    new_sum = prev_sum + int(pdf.value.sum())
    new_count = prev_count + len(pdf)

    # TODO: now it's taking second precision - lower down to millisecond
    # print(key)
    # print(pdf)
    pser = pdf.timestamp.apply(lambda dt: (int(calendar.timegm(dt.utctimetuple()) + dt.microsecond)))
    #print(pser)
    new_max_event_time_sec = int(max(pser.max(), prev_max_timestamp_seen_sec))
    timeout_timestamp_sec = new_max_event_time_sec + timeout_delay_sec

    # FIXME: Is it better UX to access the state object as tuple instead of Row or dict at least?
    state.update((new_sum, new_count, new_max_event_time_sec,))
    state.setTimeoutTimestamp(timeout_timestamp_sec * 1000)
    return pd.DataFrame({'key1': [key[0]], 'key2': [key[1]], 'maxTimestampSeenMs': [new_max_event_time_sec * 1000], 'average': [new_sum * 1.0 / new_count]})


spark = SparkSession \
  .builder \
  .appName("Python ApplyInPandasWithState example") \
  .config("spark.sql.shuffle.partitions", 1) \
  .getOrCreate()

rate_stream = (
  spark.readStream
  .format('rate')
  .option('numPartitions', 1)
  .option('rowsPerSecond', 500000)
  .load()
)

output_struct = 'key1 string, key2 long, maxTimestampSeenMs long, average double'
state_struct = 'sum long, count long, maxTimestampSeenSec long'

# desired_group_keys = 100
desired_group_keys = 100000
key1_expr = "(case when value % 5 = 0 then 'a' when value % 5 = 1 then 'b' when value % 5 = 2 then 'c' when value % 5 = 3 then 'd' else 'e' end) AS key1"
key2_expr = f"ceil(value / 5) % {desired_group_keys / 5} AS key2"

# schema from rate source: 'timestamp' - TimestampType, 'value' - LongType
custom_session_window_stream = (
  rate_stream
  # TODO: how many groups we want to track?
  .selectExpr("timestamp", key1_expr, key2_expr, "value")
  .withWatermark('timestamp', '0 seconds')
  .groupby('key1', 'key2')
  .applyInPandasWithState(user_func, outputStructType=output_struct,
    stateStructType=state_struct, outputMode='update', timeoutConf='EventTimeTimeout')
  .selectExpr('maxTimestampSeenMs * 1000', 'key1', 'key2', 'average')
)

query = (
  custom_session_window_stream
  .writeStream
  .trigger(processingTime='0 seconds')
  .outputMode('Update')
  .format('console')
  .start()
)

query.awaitTermination()


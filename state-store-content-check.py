from pyspark.sql.functions import window, col

# aggregate operator
q1 = spark.readStream.format("rate").option("rowsPerSecond", 3).load().withWatermark("timestamp", "50 seconds")\
    .groupBy(window("timestamp", "10 seconds")).count().select("window.start", "window.end", "count")\
        .writeStream.format("memory").queryName("window").option("checkpointLocation", "/tmp/state/window").trigger(processingTime="50 seconds").start()
        
# join operator
sdf1 = spark.readStream.format("rate").option("rowsPerSecond", 100).load().withWatermark("timestamp", "50 seconds")
sdf2 = spark.readStream.format("rate").option("rowsPerSecond", 100).load().withWatermark("timestamp", "50 seconds")
q2 = sdf1.join(sdf2, "timestamp").select()\
        .writeStream.format("memory").queryName("join").option("checkpointLocation", "/tmp/state/join").start()
        
# limit operator
q3 = spark.readStream.format("rate").option("rowsPerSecond", 100).load().limit(20)\
    .writeStream.format("console").queryName("limit").option("checkpointLocation", "/tmp/state/limit").start()


# rm -rf /tmp/state/window

# read from state source
meta1 = spark.read.format("state-metadata").load("/tmp/state/window")
state1 = spark.read.format("statestore").load("/tmp/state/window")


state1_1 = spark.read.format("statestore")\
    .option("snapshotStartBatchId", 11)\
        .option("snapshotPartitionId", 1)\
                .load("/Users/yuchen.liu/Desktop/spark/sql/core/src/test/resources/structured-streaming/checkpoint-version-4.0.0-state-source").show()
                
                
state1_2 = spark.read.format("statestore").option("batchId", 53).load("/tmp/state/window").show()

meta2 = spark.read.format("state-metadata").load("/tmp/state/join")
state2_1 = spark.read.format("statestore").option("storeName", "left-keyToNumValues").load("/tmp/state/join")
state2_2 = spark.read.format("statestore").option("joinSide", "left").load("/tmp/state/join")

meta3 = spark.read.format("state-metadata").load("/tmp/state/limit")
state3 = spark.read.format("statestore").load("/tmp/state/limit")

all: s1 s2 s3 s4 s5 s6 s7 s8

compile:
	 sbt test:compile

# There is likely some bug here... still a good way to get a feeling if things are working in
# parallel.
s1: compile
	 sbt ${ARGS} -Dshark.hive.shard=0:8 "test-only org.apache.spark.sql.shark.execution.HiveCompatibilitySuite"
s2: compile
	 sbt ${ARGS} -Dshark.hive.shard=1:8 "test-only org.apache.spark.sql.shark.execution.HiveCompatibilitySuite"
s3: compile
	 sbt ${ARGS} -Dshark.hive.shard=2:8 "test-only org.apache.spark.sql.shark.execution.HiveCompatibilitySuite"
s4: compile
	 sbt ${ARGS} -Dshark.hive.shard=3:8 "test-only org.apache.spark.sql.shark.execution.HiveCompatibilitySuite"
s5: compile
	 sbt ${ARGS} -Dshark.hive.shard=4:8 "test-only org.apache.spark.sql.shark.execution.HiveCompatibilitySuite"
s6: compile
	 sbt ${ARGS} -Dshark.hive.shard=5:8 "test-only org.apache.spark.sql.shark.execution.HiveCompatibilitySuite"
s7: compile
	 sbt ${ARGS} -Dshark.hive.shard=6:8 "test-only org.apache.spark.sql.shark.execution.HiveCompatibilitySuite"
s8: compile
	 sbt ${ARGS} -Dshark.hive.shard=7:8 "test-only org.apache.spark.sql.shark.execution.HiveCompatibilitySuite"

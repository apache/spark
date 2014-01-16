all: s1 s2 s3 s4 s5 s6 s7 s8

compile:
	 sbt test:compile

s1: compile
	 sbt ${ARGS} -Dshark.hive.shard=0:8 "test-only catalyst.execution.HiveCompatibility"
s2: compile
	 sbt ${ARGS} -Dshark.hive.shard=1:8 "test-only catalyst.execution.HiveCompatibility"
s3: compile
	 sbt ${ARGS} -Dshark.hive.shard=2:8 "test-only catalyst.execution.HiveCompatibility"
s4: compile
	 sbt ${ARGS} -Dshark.hive.shard=3:8 "test-only catalyst.execution.HiveCompatibility"
s5: compile
	 sbt ${ARGS} -Dshark.hive.shard=4:8 "test-only catalyst.execution.HiveCompatibility"
s6: compile
	 sbt ${ARGS} -Dshark.hive.shard=5:8 "test-only catalyst.execution.HiveCompatibility"
s7: compile
	 sbt ${ARGS} -Dshark.hive.shard=6:8 "test-only catalyst.execution.HiveCompatibility"
s8: compile
	 sbt ${ARGS} -Dshark.hive.shard=7:8 "test-only catalyst.execution.HiveCompatibility"

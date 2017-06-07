#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys

"""
Writes a record to avro file
"""
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 4 and len(sys.argv) != 5:
        print >> sys.stderr, """
        Usage: avro_outputformat <hdfs_path> <name> <favorite_color> <writer_schema_hdfs_file>

        Run with example jar locally:
        spark-submit --driver-class-path /path/to/example/jar /path/to/examples/avro_outputformat.py <path> <name> <favorite_color> <writer_schema_file>
        Writer schema is mandatory and needs to match schema used by Converter (there is no way to pass schema from PySpark to Converter due to API limitations).
        Given example is for the user.asvc schema from Spark examples jar
        
        Run on Hadoop/Spark cluster:
        spark-submit --master local --jars /path/to/jars/avro.jar,/path/to/jars/avro-mapred.jar,/path/to/jars/spark-examples.jar /path/to/avro_outputformat.py <hdfs_path> <name> <favorite_color> <user.avsc_in_hdfs>
        """
        exit(-1)

    path = sys.argv[1]
    sc = SparkContext(appName="AvroKeyOutputFormat")

    conf = None
    if len(sys.argv) == 5:
        schema_rdd = sc.textFile(sys.argv[4], 1).collect()
        conf = {"avro.schema.output.key" : reduce(lambda x, y: x+y, schema_rdd)}
    
    record = {"name" : sys.argv[2], "favorite_color" : sys.argv[3]}
    sc.parallelize([record]).map(lambda x: (x, None)).saveAsNewAPIHadoopFile(
    	path,
        "org.apache.avro.mapreduce.AvroKeyOutputFormat",
        "org.apache.avro.mapred.AvroKey",
        "org.apache.hadoop.io.NullWritable",
        keyConverter="org.apache.spark.examples.pythonconverters.UserToAvroKeyConverter",
        conf=conf)

    sc.stop()

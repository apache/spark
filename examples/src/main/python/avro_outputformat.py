import sys
from pyspark import SparkContext
import avro.schema

if __name__ == "__main__":
    avroSchema = """{"type": "record", "name": "ProductFeed", "namespace": "com.rubikloud.entity",
                  "fields": [{"name": "A", "type": "string"},
                             {"name": "B", "type":
                                 {"type": "record", "name": "B", "fields": [
                                     {"name": "B1", "type": "int"},
                                     {"name": "B2", "type": "string"},
                                     {"name": "B3", "type": ["null", "long"]}]
                                 }
                             },
                             {"name": "C", "type": ["null", {"type": "array", "items": "string"}]}
                  ]}"""

    d = [{"A": "something1",
          "B": {
              "B1": 101,
              "B2": "oneoone",
              "B3": long(456)
          }
         },
         {"A": "something2",
          "B": {
              "B1": 202,
              "B2": "twootwo"
          },
          "C": ['asd', 'sdf']
         }]

    sc = SparkContext(appName="AvroKeyInputFormat")

    conf = {
        "avro.schema.output.key": avroSchema
    }

    to_avro_rdd = sc.parallelize(d)

    to_avro_rdd.map(lambda x: ((x, avroSchema), None)).saveAsNewAPIHadoopFile(
        "test.out",
        keyClass = "org.apache.avro.mapred.AvroKey",
        valueClass = "org.apache.hadoop.io.NullWritable",
        outputFormatClass = "org.apache.avro.mapreduce.AvroKeyOutputFormat",
        keyConverter="org.apache.spark.examples.pythonconverters.JavaToAvroWrapperConverter",
        conf=conf
    )

    b = sc.newAPIHadoopFile(
        "test.out",
        "org.apache.avro.mapreduce.AvroKeyInputFormat",
        "org.apache.avro.mapred.AvroKey",
        "org.apache.hadoop.io.NullWritable",
        keyConverter="org.apache.spark.examples.pythonconverters.AvroWrapperToJavaConverter",
        conf=None).collect()

    for k in b:
        print 'read: %s' % str(k)

    sc.stop()
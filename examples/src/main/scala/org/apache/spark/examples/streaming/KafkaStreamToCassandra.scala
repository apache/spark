/**
  * Created by Saddam Khan on 4/14/2018.
  */

import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.SomeColumns

object KafkaStreamToCassandra {

        def main(args: Array[String]): Unit = {

            if (args.length < 7) {
                System.err.println("Invalid arguments list " +
                """|  Usage: KafkaStreamToCassandra <master><zookeeper><topic><consumer_group_name><numThread><hostname><keyspace_name><table_name>
                   |  <master> refers to master i.e. local[*], yarn
                   |  <zookeeper> refers to kafka zookeeper i.e. localhost:2181
                   |  <topic> refers to kafka topics to consume from i.e. topic_name
                   |  <consumer_group_name> refers to kafka consumer to consume message from kafka topic i.e. topic_consumer_group_name
                   |  <numThread> refers to number of partition in kafka topic i.e. 4
                   |  <hostname> refers to seed node ip of cassandra i.e. localhost
                   |  <keyspace_name> refers to cassandra keyspace i.e. keyspace
                   |  <table_name> refers to table under cassandra keyspace i.e. table_name #  here table is assumed to have only two columns i.e. "column1","column2"
                """)
                System.exit(1)
              }

                    // Set logger class level to WARN
                    Logger.getRootLogger.setLevel(Level.WARN)

                    // Extract program configuration argument
                    val Array(master,zookeeper,topic,consumer_group_name,numThread,hostname,keyspace_name,table_name) = args.take(7)

                    // Create sparkConf
                    val conf = new SparkConf().setAppName("spark : kafka stream to cassandra app").setMaster(master).set("spark.cassandra.connection.host",hostname)

                    // Create sparkStreamingContext
                    val ssc = new StreamingContext(conf,Seconds(10))

                    // Checkpointing to persist the DAG
                    ssc.checkpoint("checkpoint")

                    // Create kafkaConsumer
                    val kStream = KafkaUtils.createStream(ssc,zookeeper,consumer_group_name,Map(topic -> numThread.toInt))

                    // Extract value part out of KafkaMessage
                    val record = kStream.map(line => line._2)

                    // Parse record to match and fit in table( i.e. stored in cassandra) definition # here value part assumes to be text value seperated by comma i.e. "column1","column2"
                    val dbRecord = record.map(row => {
                    val arr = row.split(",")
                    (arr(0).toInt, arr(1))
                    })

                    // Print records on console which needs to be inserted to table stored in cassandra
                    dbRecord.print()

                    // Write records to cassandra table
                    dbRecord.saveToCassandra(keyspace_name, table_name, SomeColumns("column1","column2"))

                    // Start streaming context and wait until termination
                    ssc.start()
                    ssc.awaitTermination()
                    
      }

}

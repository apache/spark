package org.apache.spark.streaming.util

import org.apache.spark.sql.SparkSession
import org.junit.Test

class MockNetCatTest {
	var nc: MockNetCat = MockNetCat.start(9999);

	@Test
	def test() = {
		val spark = SparkSession.builder.master("local[4]")
			.getOrCreate();
		spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");
		import spark.implicits._

		val lines = spark.readStream
			.format("socket")
			.option("host", "localhost")
			.option("port", 9999)
			.load()

		// Split the lines into words
		val words = lines.as[String].flatMap(_.split(" "))

		// Generate running word count
		val wordCounts = words.groupBy("value").count();
		val query = wordCounts.writeStream
			.outputMode("complete")
			.format("console")
			.start()

		nc.writeData("hello\r\nworld\r\nbye\r\nworld\r\n");
		Thread.sleep(2000);
		nc.writeData("hello\r\nnetcat\r\n");
		query.awaitTermination();
	}
}


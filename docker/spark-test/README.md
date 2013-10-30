Spark Docker files usable for testing and development purposes.

These images are intended to be run like so:

	docker run -v $SPARK_HOME:/opt/spark spark-test-master
	docker run -v $SPARK_HOME:/opt/spark spark-test-worker spark://<master_ip>:7077

Using this configuration, the containers will have their Spark directories
mounted to your actual `SPARK_HOME`, allowing you to modify and recompile
your Spark source and have them immediately usable in the docker images
(without rebuilding them).

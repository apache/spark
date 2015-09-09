## Building SparkR on Windows

To build SparkR on Windows, the following steps are required

1. Install R (>= 3.1) and [Rtools](http://cran.r-project.org/bin/windows/Rtools/). Make sure to
include Rtools and R in `PATH`.
2. Install
[JDK7](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html) and set
`JAVA_HOME` in the system environment variables.
3. Download and install [Maven](http://maven.apache.org/download.html). Also include the `bin`
directory in Maven in `PATH`.
4. Set `MAVEN_OPTS` as described in [Building Spark](http://spark.apache.org/docs/latest/building-spark.html).
5. Open a command shell (`cmd`) in the Spark directory and run `mvn -DskipTests -Psparkr package`

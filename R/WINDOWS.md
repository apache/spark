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

##  Unit tests

To run existing unit tests in SparkR on Windows, the following setps are required (the steps below suppose you are in Spark root directory)

1. Set `HADOOP_HOME`.
2. Download `winutils.exe` and locate this in `$HADOOP_HOME/bin`. 

    It seems not requiring installing Hadoop but only this `winutils.exe`. It seems not included in Hadoop official binary releases so it should be built from source but it seems it is able to be downloaded from community (e.g. [steveloughran/winutils](https://github.com/steveloughran/winutils)).

3. Run unit-tests for SparkR by running below (you need to install the [testthat](http://cran.r-project.org/web/packages/testthat/index.html) package first):
 
    ```
    R -e "install.packages('testthat', repos='http://cran.us.r-project.org')"
    .\bin\spark-submit2.cmd --conf spark.hadoop.fs.defualt.name="file:///" R\pkg\tests\run-all.R
    ```

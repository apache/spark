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

5. Open a command shell (`cmd`) in the Spark directory and build Spark with [Maven](http://spark.apache.org/docs/latest/building-spark.html#building-with-buildmvn) and include the `-Psparkr` profile to build the R package. For example to use the default Hadoop versions you can run

    ```bash
    mvn.cmd -DskipTests -Psparkr package
    ```

    `.\build\mvn` is a shell script so `mvn.cmd` should be used directly on Windows.

##  Unit tests

To run the SparkR unit tests on Windows, the following steps are required —assuming you are in the Spark root directory and do not have Apache Hadoop installed already:

1. Create a folder to download Hadoop related files for Windows. For example, `cd ..` and `mkdir hadoop`.

2. Download the relevant Hadoop bin package from [steveloughran/winutils](https://github.com/steveloughran/winutils). While these are not official ASF artifacts, they are built from the ASF release git hashes by a Hadoop PMC member on a dedicated Windows VM. For further reading, consult [Windows Problems on the Hadoop wiki](https://wiki.apache.org/hadoop/WindowsProblems).

3. Install the files into `hadoop\bin`; make sure that `winutils.exe` and `hadoop.dll` are present.

4. Set the environment variable `HADOOP_HOME` to the full path to the newly created `hadoop` directory.

5. Run unit tests for SparkR by running the command below. You need to install the [testthat](http://cran.r-project.org/web/packages/testthat/index.html) package first:

    ```
    R -e "install.packages('testthat', repos='http://cran.us.r-project.org')"
    .\bin\spark-submit2.cmd --conf spark.hadoop.fs.default.name="file:///" R\pkg\tests\run-all.R
    ```


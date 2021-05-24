---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

## Building SparkR on Windows

To build SparkR on Windows, the following steps are required

1. Make sure `bash` is available and in `PATH` if you already have a built-in `bash` on Windows. If you do not have, install [Cygwin](https://www.cygwin.com/).

2. Install R (>= 3.5) and [Rtools](https://cloud.r-project.org/bin/windows/Rtools/). Make sure to
include Rtools and R in `PATH`.

3. Install JDK that SparkR supports (see `R/pkg/DESCRIPTION`), and set `JAVA_HOME` in the system environment variables.

4. Download and install [Maven](https://maven.apache.org/download.html). Also include the `bin`
directory in Maven in `PATH`.

5. Set `MAVEN_OPTS` as described in [Building Spark](https://spark.apache.org/docs/latest/building-spark.html).

6. Open a command shell (`cmd`) in the Spark directory and build Spark with [Maven](https://spark.apache.org/docs/latest/building-spark.html#buildmvn) and include the `-Psparkr` profile to build the R package. For example to use the default Hadoop versions you can run

    ```bash
    mvn.cmd -DskipTests -Psparkr package
    ```

    Note that `.\build\mvn` is a shell script so `mvn.cmd` on the system should be used directly on Windows.
    Make sure your Maven version is matched to `maven.version` in `./pom.xml`.

Note that it is a workaround for SparkR developers on Windows. Apache Spark does not officially support to _build_ on Windows yet whereas it supports to _run_ on Windows.

##  Unit tests

To run the SparkR unit tests on Windows, the following steps are required —assuming you are in the Spark root directory and do not have Apache Hadoop installed already:

1. Create a folder to download Hadoop related files for Windows. For example, `cd ..` and `mkdir hadoop`.

2. Download the relevant Hadoop bin package from [steveloughran/winutils](https://github.com/steveloughran/winutils). While these are not official ASF artifacts, they are built from the ASF release git hashes by a Hadoop PMC member on a dedicated Windows VM. For further reading, consult [Windows Problems on the Hadoop wiki](https://wiki.apache.org/hadoop/WindowsProblems).

3. Install the files into `hadoop\bin`; make sure that `winutils.exe` and `hadoop.dll` are present.

4. Set the environment variable `HADOOP_HOME` to the full path to the newly created `hadoop` directory.

5. Run unit tests for SparkR by running the command below. You need to install the needed packages following the instructions under [Running R Tests](https://spark.apache.org/docs/latest/building-spark.html#running-r-tests) first:

    ```
    .\bin\spark-submit2.cmd --conf spark.hadoop.fs.defaultFS="file:///" R\pkg\tests\run-all.R
    ```


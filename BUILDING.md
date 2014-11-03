## Building SparkR on Windows

To build SparkR on Windows, the following steps are required

1. Install R (>= 3.1) and [Rtools](http://cran.r-project.org/bin/windows/Rtools/). Make sure to
include Rtools and R in `PATH`.
2. Install
[JDK7](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html) and set
`JAVA_HOME` in the system environment variables.
3. Install `rJava` using `install.packages(rJava)`. If rJava fails to load due to missing jvm.dll,
you will need to add the directory containing jvm.dll to `PATH`. See this [stackoverflow post](http://stackoverflow.com/a/7604469]
for more details.
4. Download and install [Maven](http://maven.apache.org/download.html). Also include the `bin`
directory in Maven in `PATH`.
5. Get SparkR source code either using [`git]`(http://git-scm.com/downloads) or by downloading a
source zip from github.
6. Open a command shell (`cmd`) in the SparkR directory and run `install-dev.bat`

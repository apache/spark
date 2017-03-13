---
layout: global
title: Groovy Programming Guide
---

* This will become a table of contents (this text will be scraped).
{:toc}

#Overview
This page discusses the benefits of using Groovy bindings for Spark and explains how to do it “out of the box”.

#Motivation
[Groovy](http://groovy.codehaus.org/) is a dynamic JVM-language which significantly reduces the code bloat of Java (its original intention was to achieve the simplicity and compactness of Python on the JVM).  At the same time, it adds some useful concepts like closures, dynamic typing, and more.<br>
Compared to Scala, Groovy is much easier to learn due to more intuitive syntax which is essentially a “simplified Java” (in fact, Groovy is to a large degree a superset of Java – if in doubt, a programmer can fall back to a standard Java syntax). While Java is an industry approved and robust language, programs tend to contain a lot of boilerplate code, especially in case of functional programming APIs [^1].<br>
Groovy as a third JVM-based language for Spark has potential to combine the expressiveness of Scala with simplicity and low learning effort of a “Python-like” programming language. In context of Spark particularly helpful are the Groovy Closures which work like anonymous functions in Scala. Since Groovy version 2.2 they can be [automatically casted](http://groovy.codehaus.org/Groovy+2.2+release+notes#Groovy2.2releasenotes-Implicitclosurecoercion) to interfaces with a single method. This allows using Spark’s native Java API directly from Groovy, making the code more readable and easier to write. Furthermore, due to this feature no “adapter code” is needed – except for a changed build process, Groovy can be used in Spark “out of the box”.<br>
Just compare this Groovy code to a Java equivalent:

<div class="codetabs">
<div data-lang="Groovy" markdown="1">
{% highlight Groovy %}
def words = lines.flatMap({ s -> Arrays.asList(SPACE.split(s))})
def counts = words.mapToPair({ new Tuple2(it, 1) }).reduceByKey({ a, b -> a + b })
{% endhighlight %}


</div>
<div data-lang="Java" markdown="1">
{% highlight Java %}
JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
  @Override
  public Iterable<String> call(String s) {
    return Arrays.asList(SPACE.split(s));
  }
});

JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
  @Override
  public Tuple2<String, Integer> call(String s) {
    return new Tuple2<String, Integer>(s, 1);
  }
});

JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
  @Override
  public Integer call(Integer i1, Integer i2) {
    return i1 + i2;
  }
});
{% endhighlight %}

</div>
</div>

#Installation
In order to use Groovy4Spark make sure that you have Groovy version 2.2 or higher installed. If this is already the case, set the environment variable GROOVY_HOME to the path of the installation directory (Linux, bash shell):

{% highlight bash %}
$ export GROOVY_HOME=~/dev/groovy
# To check whether the variable is set:
$ echo $GROOVY_HOME
{% endhighlight %}

On Linux, Groovy installation is straightforward:

{% highlight bash %}
# Get the Groovy enVironment Manager
$ curl -s get.gvmtool.net | bash
# CAUTION: Before you can use gvm you need to restart your bash
# Install Groovy
$ gvm install groovy
{% endhighlight %}

When everything works, you are now good to run the Groovy examples like the following one (for more examples see the git repo):
{% highlight bash %}
$ ./bin/run-groovy-example GroovySparkPi
{% endhighlight %}

#Standalone Application
To illustrate how easy it is to create a standalone application with Groovy for Spark, we show a simple program that counts lines with “a”s and “b”s in a text file. In order to deploy it, we will build the executable jar file with [Gradle](http://www.gradle.org/), a groovish build/test/deploy automation tool.

###Directory structure
First we need to create a new directory for our app. Our build tool Gradle requires a certain directory structure. Make sure that your directory structure is as follows:

{% highlight bash %}
$ find .
./build.gradle
./src
./src/main
./src/main/groovy
./src/main/groovy/SimpleApp.groovy
{% endhighlight %}

As next we describe the contents of `SimpleApp.groovy` and `build.groovy`.

###SimpleApp.groovy
The following program is a demonstration how to use Groovy with Spark. It reads a file and counts the lines which contain “a” or “b”. For a comparison with other programming languages for Spark please see [here](quick-start.html#standalone-applications).<br>
CAUTION: To run this code you have to replace the string YOUR_SPARK_HOME with the location where your Spark version is installed.

{% highlight groovy %}
/* SimpleApp.groovy */
import org.apache.spark.api.java.*
import org.apache.spark.SparkConf

class SimpleApp {
  static def main(args) {
    // Should be some file on your system, for example:
    def logFile = "YOUR_SPARK_HOME/README.md" 
    def conf = new SparkConf().setAppName("Simple Groovy Application")
    def sc = new JavaSparkContext(conf)
    def logData = sc.textFile(logFile).cache()

    def numAs = logData.filter({it.contains("a")}).count()

    def numBs = logData.filter({it.contains("b")}).count()

    println("Lines with a: $numAs, lines with b: $numBs")
  }
}

{% endhighlight %}

As you can see the implementation is pretty straightforward and although we use Groovy, we deploy the Spark’s standard Java API.

###build.gradle
Spark requires jar files while executing in a cluster. In order to build a jar file we will use Gradle. This automation tool can be installed via:

{% highlight bash %}
# Skip the next line if your Groovy enVironment Manager (gvm) is installed
$ curl -s get.gvmtool.net | bash
# Install Gradle
$ gvm install gradle
{% endhighlight %}

The file `build.gradle` specifies which external `jars` are needed (Maven-functionality) and names the resulting `jar`. Of course, the version numbers can be adjusted for the most recent version.

{% highlight groovy %}
apply plugin: 'groovy'

repositories {
    mavenCentral()
}

dependencies {
    compile 'org.codehaus.groovy:groovy-all:2.2.0'
    compile 'org.apache.spark:spark-core_2.10:1.0.0'
}

jar.archiveName = "simple_app.jar"
{% endhighlight %}


###Running the Application
As the last step we package our app by executing `gradle build` command and then run the created jar with the script spark-submit:

CAUTION: You need to replace the strings the YOUR_GROOVY_VERSION and LOCATION_OF_PROJECT to match your local setup first.

{% highlight bash %}
# Packaging our app. This will create a new jar in the folder ./build/libs
$ gradle build
# Submit our app to Spark
$ YOUR_SPARK_HOME/bin/spark-submit \
	--jars $GROOVY_HOME/embeddable/groovy-all-YOUR_GROOVY_VERSION.jar \
	--class "SimpleApp" \
	--master local[*] \
	LOCATION_OF_PROJECT/simple_app/build/libs/simple_app.jar 
{% endhighlight %}
Please adopt the `YOUR_GROOVY_VERSION` and `LOCATION_OF_PROJECT` to match your setup.

Congratulations, you have executed your first application with Groovy for Spark!

#Feedback
If you have comments, suggestions for improvements, or any questions, you are encouraged to contact us directly: Constantin Ahlmann-Eltze or Artur Andrzejak at the Parallel and Distributed Systems Group ([PVS](http://pvs.ifi.uni-heidelberg.de/home/)), Heidelberg University.<br><br>

----

[^1]: This will probably improve with the adoption of [Java 8](http://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html)
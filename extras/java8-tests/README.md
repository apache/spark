# Java 8 Test Suites

These tests require having Java 8 installed and are isolated from the main Spark build.
If Java 8 is not your system's default Java version, you will need to point Spark's build
to your Java location. The set-up depends a bit on the build system:

* Sbt users can either set JAVA_HOME to the location of a Java 8 JDK or explicitly pass
  `-java-home` to the sbt launch script. If a Java 8 JDK is detected sbt will automatically
  include the Java 8 test project.

  `$ JAVA_HOME=/opt/jdk1.8.0/ build/sbt clean "test-only org.apache.spark.Java8APISuite"`

* For Maven users,

  Maven users can also refer to their Java 8 directory using JAVA_HOME. However, Maven will not
  automatically detect the presence of a Java 8 JDK, so a special build profile `-Pjava8-tests`
  must be used.

  `$ JAVA_HOME=/opt/jdk1.8.0/ mvn clean install -DskipTests`
  `$ JAVA_HOME=/opt/jdk1.8.0/ mvn test -Pjava8-tests -DwildcardSuites=org.apache.spark.Java8APISuite`

  Note that the above command can only be run from project root directory since this module
  depends on core and the test-jars of core and streaming. This means an install step is
  required to make the test dependencies visible to the Java 8 sub-project.

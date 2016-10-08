# Java 8 Test Suites

These tests require having Java 8 installed and are isolated from the main Spark build.
If Java 8 is not your system's default Java version, you will need to point Spark's build
to your Java location. The set-up depends a bit on the build system:

* Sbt users can either set JAVA_HOME to the location of a Java 8 JDK or explicitly pass
  `-java-home` to the sbt launch script. If a Java 8 JDK is detected sbt will automatically
  include the Java 8 test project.

  `$ JAVA_HOME=/opt/jdk1.8.0/ build/sbt clean java8-tests/test

* For Maven users,

  Maven users can also refer to their Java 8 directory using JAVA_HOME.

  `$ JAVA_HOME=/opt/jdk1.8.0/ mvn clean install -DskipTests`
  `$ JAVA_HOME=/opt/jdk1.8.0/ mvn -pl :java8-tests_2.11 test`

  Note that the above command can only be run from project root directory since this module
  depends on core and the test-jars of core and streaming. This means an install step is
  required to make the test dependencies visible to the Java 8 sub-project.

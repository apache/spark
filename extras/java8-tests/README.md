# Java 8 test suites.

These tests are bundled with spark and run if you have java 8 installed as system default or your `JAVA_HOME` points to a java 8(or higher) installation. `JAVA_HOME` is preferred to system default jdk installation. Since these tests require jdk 8 or higher, they defined to be optional to run in the build system.

* For sbt users, it automatically detects the presence of java 8 based on either `JAVA_HOME` environment variable or default installed jdk and run these tests. It takes highest precednce, if java home is passed as follows.

  `$ sbt/sbt -java-home "/path/to/jdk1.8.0"`

* For maven users,

  This automatic detection is not possible and thus user has to ensure that either `JAVA_HOME` environment variable or default installed jdk points to jdk 8.

  `$ mvn install -Pjava8-tests`

  Above command can only be run from project root directory since this module depends on both core and test-jars of core and streaming. These jars are installed first time the above command is run as java8-tests profile enables local publishing of test-jar artifacts as well. Once these artifacts are published then these tests can be run from this module's directory as well.

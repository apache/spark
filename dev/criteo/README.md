## Criteo specific developer's scripts ##

This directory holds a bunch of script intended for contributing to Spark from the Criteo environment. 
The content of this directory *is not to be contributed upstream*.

### Local compilation and testing ###

* criteo-spark-env.sh: to be *sourc'ed* by other scrip to provide consistent configuration
* build-with-maven.sh: package Spark using *the right* (really?) options for the Criteo build of Spark. 
  Additional argument can be passed, they will be forwarded to maven. Tests **are skipped**.
* test-with-maven.sh: launch Spark unit testing with options for the Criteo build of Spark. 
  Additional argument can be passed, they will be forwarded to maven. Most useful one are
  
  ```
  ./dev/criteo/build-with-maven -pl mllib # Run tesst only for one maven module
  ./dev/criteo/build-with-maven -Dtest=Those*TesSuite,!ButNotThiOne # Select tests
  ```

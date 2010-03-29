EMPTY =
SPACE = $(EMPTY) $(EMPTY)

# Build up classpath by concatenating some strings
JARS = third_party/nexus.jar
JARS += third_party/asm-3.2/lib/all/asm-all-3.2.jar
JARS += third_party/colt.jar
JARS += third_party/google-collect-1.0-rc5/google-collect-1.0-rc5.jar
JARS += third_party/hadoop-0.20.0/hadoop-0.20.0-core.jar
JARS += third_party/hadoop-0.20.0/lib/commons-logging-1.0.4.jar
JARS += third_party/scalatest-1.0/scalatest-1.0.jar
JARS += third_party/ScalaCheck-1.5.jar
CLASSPATH = $(subst $(SPACE),:,$(JARS))

SCALA_SOURCES =  src/examples/*.scala src/scala/spark/*.scala src/scala/spark/repl/*.scala
SCALA_SOURCES += src/test/spark/*.scala src/test/spark/repl/*.scala

JAVA_SOURCES = $(wildcard src/java/spark/compress/lzf/*.java)

ifeq ($(USE_FSC),1)
  COMPILER_NAME = fsc
else
  COMPILER_NAME = scalac
endif

ifeq ($(SCALA_HOME),)
  COMPILER = $(COMPILER_NAME)
else
  COMPILER = $(SCALA_HOME)/bin/$(COMPILER_NAME)
endif

all: scala java

build/classes:
	mkdir -p build/classes

scala: build/classes java
	$(COMPILER) -unchecked -d build/classes -classpath $(CLASSPATH) $(SCALA_SOURCES)

java: $(JAVA_SOURCES) build/classes
	javac -d build/classes $(JAVA_SOURCES)

native: java
	$(MAKE) -C src/native

jar: build/spark.jar build/spark-dep.jar

build/spark.jar: scala java
	jar cf build/spark.jar -C build/classes spark

build/spark-dep.jar:
	mkdir -p build/dep
	cd build/dep &&	for i in $(JARS); do jar xf ../../$$i; done
	jar cf build/spark-dep.jar -C build/dep .

test: all
	./alltests

default: all

clean:
	$(MAKE) -C src/native clean
	rm -rf build

.phony: default all clean scala java native jar

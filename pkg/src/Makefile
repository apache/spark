SCALA_VERSION := 2.10
JAR_NAME := sparkr-assembly-0.1.jar
SBT_TARGET_NAME := target/scala-$(SCALA_VERSION)/$(JAR_NAME)

MAVEN_JAR_NAME := sparkr-0.1-assembly.jar
MAVEN_TARGET_NAME := target/$(MAVEN_JAR_NAME)

SCALA_SOURCE_DIR := src/main/scala/edu/berkeley/cs/amplab/sparkr
RESOURCE_DIR := src/main/resources

SCALA_FILES := $(wildcard $(SCALA_SOURCE_DIR)/*.scala)
RESOURCE_FILES := $(wildcard $(RESOURCE_DIR)/*)

SPARK_HADOOP_VERSION ?= 1.0.4

ifdef USE_MAVEN
	TARGET_NAME := $(MAVEN_TARGET_NAME)
	BUILD_TOOL := mvn
else
	TARGET_NAME := $(SBT_TARGET_NAME)
	BUILD_TOOL := sbt/sbt
endif

all: $(TARGET_NAME)

$(SBT_TARGET_NAME): $(SCALA_FILES) $(RESOURCE_FILES)
	./sbt/sbt assembly
	cp $(SBT_TARGET_NAME) ../inst/

$(MAVEN_TARGET_NAME): $(SCALA_FILES) $(RESOURCE_FILES)
	mvn -Dhadoop.version=$(SPARK_HADOOP_VERSION) -DskipTests clean package shade:shade
	cp $(MAVEN_TARGET_NAME) ../inst/$(JAR_NAME)

clean:
	$(BUILD_TOOL) clean
	rm -rf target
	rm -rf project/target
	rm -rf project/project
	-rm sbt/sbt-launch-*.jar
	rm ../inst/$(JAR_NAME)

.PHONY: all clean

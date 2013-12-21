LIB_DIR := $(shell pwd)/lib/
SCALA_VERSION := 2.10
JAR_NAME := SparkR-assembly-0.1.jar

all: sparkR

SparkR: pkg/R/* target/scala-$(SCALA_VERSION)/*.jar
	cp target/scala-$(SCALA_VERSION)/$(JAR_NAME) pkg/inst/
	mkdir -p $(LIB_DIR)
	R CMD INSTALL --library=$(LIB_DIR) pkg/

sparkR: SparkR

sparkr: SparkR

LIB_DIR := lib/
JAR_NAME := sparkr-1.0-allinone.jar

all: sparkR

SparkR: pkg/R/*
	R CMD INSTALL --library=$(LIB_DIR) pkg/

sparkR: SparkR

sparkr: SparkR

#java:
	#mvn package shade:shade

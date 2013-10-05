LIB_DIR := $(shell pwd)/lib/

all: sparkR

SparkR: pkg/R/*
	R CMD INSTALL --library=$(LIB_DIR) pkg/

sparkR: SparkR

sparkr: SparkR

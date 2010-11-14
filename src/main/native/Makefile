CC = gcc
#JAVA_HOME = /usr/lib/jvm/java-6-sun
OS_NAME = linux

CFLAGS += -fPIC -O3 -funroll-all-loops

SPARK = ../../..

LZF = $(SPARK)/lib/liblzf-3.5

TARGET = $(SPARK)/target/scala_2.8.1

LIB = $(TARGET)/native/libspark_native.so

OS_X_LIB = $(TARGET)/native/libspark_native.dylib

all: $(LIB)

spark_compress_lzf_LZF.h: $(TARGET)/classes/spark/compress/lzf/LZF.class
ifeq ($(JAVA_HOME),)
	$(error JAVA_HOME is not set)
else
	$(JAVA_HOME)/bin/javah -classpath $(SPARK)/target/scala_2.8.1/classes spark.compress.lzf.LZF
endif

$(TARGET)/native:
	mkdir -p $@

$(LIB): spark_compress_lzf_LZF.h spark_compress_lzf_LZF.c | $(TARGET)/native
	$(CC) $(CFLAGS) -shared -o $@ spark_compress_lzf_LZF.c \
	      -I $(JAVA_HOME)/include -I $(JAVA_HOME)/include/$(OS_NAME) \
	      -I $(LZF) $(LZF)/lzf_c.c $(LZF)/lzf_d.c

$(OS_X_LIB): $(LIB)
	cp $< $@

clean:
	rm -f spark_compress_lzf_LZF.h $(LIB)

.PHONY: all clean

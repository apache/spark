CC = gcc
#JAVA_HOME = /usr/lib/jvm/java-6-sun
OS_NAME = linux

CFLAGS += -fPIC -O3 -funroll-all-loops

SPARK = ../../..

LZF = $(SPARK)/lib/liblzf-3.5

LIB = libspark_native.so

all: $(LIB)

spark_compress_lzf_LZF.h: $(SPARK)/target/scala_2.8.1/classes/spark/compress/lzf/LZF.class
ifeq ($(JAVA_HOME),)
	$(error JAVA_HOME is not set)
else
	$(JAVA_HOME)/bin/javah -classpath $(SPARK)/target/scala_2.8.1/classes spark.compress.lzf.LZF
endif

$(LIB): spark_compress_lzf_LZF.h spark_compress_lzf_LZF.c
	$(CC) $(CFLAGS) -shared -o $@ spark_compress_lzf_LZF.c \
	      -I $(JAVA_HOME)/include -I $(JAVA_HOME)/include/$(OS_NAME) \
	      -I $(LZF) $(LZF)/lzf_c.c $(LZF)/lzf_d.c

clean:
	rm -f spark_compress_lzf_LZF.h $(LIB)

.PHONY: all clean

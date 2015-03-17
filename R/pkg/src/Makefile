
all: sharelib

sharelib: string_hash_code.c
	R CMD SHLIB -o SparkR.so string_hash_code.c

clean:
	rm -f *.o
	rm -f *.so
       
.PHONY: all clean

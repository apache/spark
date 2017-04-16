To run the examples, first compile them:

% ant -Dcompile.c++=yes examples

and then copy the binaries to dfs:

% bin/hadoop fs -put build/c++-examples/Linux-i386-32/bin /examples/bin

create an input directory with text files:

% bin/hadoop fs -put my-data in-dir

and run the word count example:

% bin/hadoop pipes -conf src/examples/pipes/conf/word.xml \
                   -input in-dir -output out-dir

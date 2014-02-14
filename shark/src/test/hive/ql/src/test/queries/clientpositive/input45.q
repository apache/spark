SET hive.insert.into.multilevel.dirs=true;

SET hive.output.file.extension=.txt;

INSERT OVERWRITE DIRECTORY '../build/ql/test/data/x/y/z/' SELECT src.* FROM src;

dfs -cat ../build/ql/test/data/x/y/z/*.txt;

dfs -rmr ../build/ql/test/data/x;
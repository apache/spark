set hive.fetch.task.conversion=more;

select ewah_bitmap_empty(array(13,2,4,8589934592,0,0)) from src tablesample (1 rows);

select ewah_bitmap_empty(array(13,2,4,8589934592,4096,0)) from src tablesample (1 rows);

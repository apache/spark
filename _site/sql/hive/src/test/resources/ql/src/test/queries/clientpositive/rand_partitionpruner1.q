-- scanning un-partitioned data
explain extended select * from src where rand(1) < 0.1;
select * from src where rand(1) < 0.1;

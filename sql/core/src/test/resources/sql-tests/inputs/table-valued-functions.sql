-- unresolved function
select * from dummy(3);

-- range call with end
select * from range(5);

-- range call with start and end
select * from range(5, 10);

-- range call with step
select * from range(0, 10, 2);

-- range call with numPartitions
select * from range(0, 10, 1, 200);

-- range call error
select * from range(1, 1, 1, 1, 1);

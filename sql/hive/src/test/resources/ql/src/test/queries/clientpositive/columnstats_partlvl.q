
DROP TABLE Employee_Part;

CREATE TABLE Employee_Part(employeeID int, employeeName String) partitioned by (employeeSalary double)
row format delimited fields terminated by '|'  stored as textfile;

LOAD DATA LOCAL INPATH "../data/files/employee.dat" INTO TABLE Employee_Part partition(employeeSalary=2000.0);
LOAD DATA LOCAL INPATH "../data/files/employee.dat" INTO TABLE Employee_Part partition(employeeSalary=4000.0);

explain 
analyze table Employee_Part partition (employeeSalary=2000.0) compute statistics for columns employeeID;
explain extended
analyze table Employee_Part partition (employeeSalary=2000.0) compute statistics for columns employeeID;
analyze table Employee_Part partition (employeeSalary=2000.0) compute statistics for columns employeeID;

explain 
analyze table Employee_Part partition (employeeSalary=4000.0) compute statistics for columns employeeID;
explain extended
analyze table Employee_Part partition (employeeSalary=4000.0) compute statistics for columns employeeID;
analyze table Employee_Part partition (employeeSalary=4000.0) compute statistics for columns employeeID;

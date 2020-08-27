-- test cases for array functions

CREATE temporary view basic_pays as select * from values
  ('Diane Murphy','Accounting',8435),
	('Mary Patterson','Accounting',9998),
	('Jeff Firrelli','Accounting',8992),
	('William Patterson','Accounting',8870),
	('Gerard Bondur','Accounting',11472),
	('Anthony Bow','Accounting',6627),
	('Leslie Jennings','IT',8113),
	('Leslie Thompson','IT',5186),
	('Julie Firrelli','Sales',9181),
	('Steve Patterson','Sales',9441),
	('Foon Yue Tseng','Sales',6660),
	('George Vanauf','Sales',10563),
	('Loui Bondur','SCM',10449),
	('Gerard Hernandez','SCM',6949),
	('Pamela Castillo','SCM',11303),
	('Larry Bott','SCM',11798),
	('Barry Jones','SCM',10586)
as basic_pays(
    employee_name,
    department,
    salary
);

SELECT
    employee_name,
    salary,
    NTH_VALUE(employee_name, 2) OVER  (ORDER BY salary DESC ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) second_highest_salary,
    NTH_VALUE(employee_name, 2) OVER (ORDER BY salary DESC) second_highest_salary2
FROM
    basic_pays;

drop view basic_pays;

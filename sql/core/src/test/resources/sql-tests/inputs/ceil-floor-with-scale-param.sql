-- Tests different scenarios of ceil and floor functions with scale parameters
SELECT CEIL(2.5, 0);
SELECT CEIL(3.5, 0);
SELECT CEIL(-2.5, 0);
SELECT CEIL(-3.5, 0);
SELECT CEIL(-0.35, 1);
SELECT CEIL(-35, -1);
SELECT CEIL(-0.1, 0);
SELECT CEIL(5, 0);
SELECT CEIL(3.14115, -3);
SELECT CEIL(2.5, null);
SELECT CEIL(2.5, 'a');
SELECT CEIL(2.5, 0, 0);

-- Same inputs with floor function
SELECT FLOOR(2.5, 0);
SELECT FLOOR(3.5, 0);
SELECT FLOOR(-2.5, 0);
SELECT FLOOR(-3.5, 0);
SELECT FLOOR(-0.35, 1);
SELECT FLOOR(-35, -1);
SELECT FLOOR(-0.1, 0);
SELECT FLOOR(5, 0);
SELECT FLOOR(3.14115, -3);
SELECT FLOOR(2.5, null);
SELECT FLOOR(2.5, 'a');
SELECT FLOOR(2.5, 0, 0);
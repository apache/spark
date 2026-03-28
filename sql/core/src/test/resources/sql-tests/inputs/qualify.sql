-- [SPARK-43413] Support QUALIFY clause in Spark SQL

CREATE TABLE dealer (id INT, city STRING, car_model STRING, quantity INT) USING parquet;
INSERT INTO dealer VALUES
    (100, 'Fremont', 'Honda Civic', 10),
    (100, 'Fremont', 'Honda Accord', 15),
    (100, 'Fremont', 'Honda CRV', 7),
    (200, 'Dublin', 'Honda Civic', 20),
    (200, 'Dublin', 'Honda Accord', 10),
    (200, 'Dublin', 'Honda CRV', 3),
    (300, 'San Jose', 'Honda Civic', 5),
    (300, 'San Jose', 'Honda Accord', 8);

-- QUALIFY with window function in SELECT list
SELECT id, city, car_model, quantity, rank() OVER (PARTITION BY city ORDER BY quantity DESC) as rank
FROM dealer
QUALIFY rank <= 2;

-- QUALIFY with window function in QUALIFY clause
SELECT id, city, car_model, quantity
FROM dealer
QUALIFY rank() OVER (PARTITION BY city ORDER BY quantity DESC) <= 2;

-- QUALIFY with WHERE and GROUP BY
SELECT city, sum(quantity) as total_quantity
FROM dealer
WHERE id < 300
GROUP BY city
QUALIFY rank() OVER (ORDER BY total_quantity DESC) = 1;

-- QUALIFY with pipe operator
SELECT id, city, car_model, quantity FROM dealer
|> QUALIFY rank() OVER (PARTITION BY city ORDER BY quantity DESC) <= 1;

-- Error: QUALIFY without window function
SELECT id, city, car_model, quantity
FROM dealer
QUALIFY quantity > 10;

-- Error: QUALIFY with aggregate function
SELECT city, sum(quantity)
FROM dealer
QUALIFY sum(quantity) > 10;

DROP TABLE dealer;

SELECT
  bucket1,
  bucket2,
  bucket3,
  bucket4,
  bucket5
FROM
  (SELECT CASE WHEN count1 > 62316685
    THEN then1
          ELSE else1 END bucket1
  FROM (
         SELECT
           count(*) count1,
           avg(ss_ext_sales_price) then1,
           avg(ss_net_paid_inc_tax) else1
         FROM store_sales
         WHERE ss_quantity BETWEEN 1 AND 20
       ) A1) B1
  CROSS JOIN
  (SELECT CASE WHEN count2 > 19045798
    THEN then2
          ELSE else2 END bucket2
  FROM (
         SELECT
           count(*) count2,
           avg(ss_ext_sales_price) then2,
           avg(ss_net_paid_inc_tax) else2
         FROM store_sales
         WHERE ss_quantity BETWEEN 21 AND 40
       ) A2) B2
  CROSS JOIN
  (SELECT CASE WHEN count3 > 365541424
    THEN then3
          ELSE else3 END bucket3
  FROM (
         SELECT
           count(*) count3,
           avg(ss_ext_sales_price) then3,
           avg(ss_net_paid_inc_tax) else3
         FROM store_sales
         WHERE ss_quantity BETWEEN 41 AND 60
       ) A3) B3
  CROSS JOIN
  (SELECT CASE WHEN count4 > 216357808
    THEN then4
          ELSE else4 END bucket4
  FROM (
         SELECT
           count(*) count4,
           avg(ss_ext_sales_price) then4,
           avg(ss_net_paid_inc_tax) else4
         FROM store_sales
         WHERE ss_quantity BETWEEN 61 AND 80
       ) A4) B4
  CROSS JOIN
  (SELECT CASE WHEN count5 > 184483884
    THEN then5
          ELSE else5 END bucket5
  FROM (
         SELECT
           count(*) count5,
           avg(ss_ext_sales_price) then5,
           avg(ss_net_paid_inc_tax) else5
         FROM store_sales
         WHERE ss_quantity BETWEEN 81 AND 100
       ) A5) B5
  CROSS JOIN
  reason
WHERE r_reason_sk = 1

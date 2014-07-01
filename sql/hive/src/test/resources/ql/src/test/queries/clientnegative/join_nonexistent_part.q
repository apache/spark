SET hive.security.authorization.enabled = true;
SELECT *
FROM srcpart s1 join src s2 on s1.key == s2.key
WHERE s1.ds='non-existent';
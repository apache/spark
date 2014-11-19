
compile `import org.apache.hadoop.hive.ql.exec.UDF \;
public class Pyth extends UDF {
  public double evaluate(double a, double b){
    return Math.sqrt((a*a) + (b*b)) \;
  }
} ` AS GROOVY NAMED Pyth.groovy;
CREATE TEMPORARY FUNCTION Pyth as 'Pyth';

SELECT Pyth(3,4) FROM src tablesample (1 rows);

DROP TEMPORARY FUNCTION Pyth;


compile `import org.apache.hadoop.hive.ql.exec.UDF \;
public class Pyth extsfgsfgfsends UDF {
  public double evaluate(double a, double b){
    return Math.sqrt((a*a) + (b*b)) \;
  }
} ` AS GROOVY NAMED Pyth.groovy;


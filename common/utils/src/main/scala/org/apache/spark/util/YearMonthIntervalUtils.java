package org.apache.spark.util;

// Replicating code from SparkIntervalUtils so code in the 'common' space can work with
// year-month intervals.
public class YearMonthIntervalUtils {
  private static byte YEAR = 0;
  private static byte MONTH = 1;
  private static int MONTHS_PER_YEAR = 12;

  public static String toYearMonthIntervalANSIString(int months, byte startField, byte endField) {
    String sign = "";
    long absMonths = months;
    if (months < 0) {
      sign = "-";
      absMonths = -absMonths;
    }
    String year = sign + Long.toString(absMonths / MONTHS_PER_YEAR);
    String yearAndMonth = year + "-" + Long.toString(absMonths % MONTHS_PER_YEAR);
    StringBuilder formatBuilder = new StringBuilder("INTERVAL '");
    if (startField == endField) {
      if (startField == YEAR) {
        formatBuilder.append(year + "' YEAR");
      } else {
        formatBuilder.append(Integer.toString(months) + "' MONTH");
      }
    } else {
      formatBuilder.append(yearAndMonth + "' YEAR TO MONTH");
    }
    return formatBuilder.toString();
  }
}

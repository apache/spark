package org.apache.spark.sql;


import org.apache.spark.sql.OutputMode;
import org.junit.After;
import org.junit.Test;


public class JavaOutputModeSuite {

  @Test
  public void testOutputModes() {
    OutputMode o1 = OutputMode.Append();
    assert(o1.toString().toLowerCase().contains("append"));
    OutputMode o2 = OutputMode.Complete();
    assert (o2.toString().toLowerCase().contains("complete"));
  }
}

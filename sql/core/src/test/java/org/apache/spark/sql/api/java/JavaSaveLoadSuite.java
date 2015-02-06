package org.apache.spark.sql.api.java;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.sql.sources.*;

public class JavaSaveLoadSuite {

  @Test
  public void getSaveMode() {
    SaveMode append = SaveModes.Append;
    Assert.assertTrue(append == SaveModes.Append);
    Assert.assertTrue(append != SaveModes.Overwrite);
    Assert.assertTrue(append != SaveModes.ErrorIfExists);

    SaveMode overwrite = SaveModes.Overwrite;
    Assert.assertTrue(overwrite == SaveModes.Overwrite);
    Assert.assertTrue(overwrite != SaveModes.Append);
    Assert.assertTrue(overwrite != SaveModes.ErrorIfExists);

    SaveMode errorIfExists = SaveModes.ErrorIfExists;
    Assert.assertTrue(errorIfExists == SaveModes.ErrorIfExists);
    Assert.assertTrue(errorIfExists != SaveModes.Append);
    Assert.assertTrue(errorIfExists != SaveModes.Overwrite);
  }
}

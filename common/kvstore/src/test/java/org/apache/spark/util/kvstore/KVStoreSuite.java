package org.apache.spark.util.kvstore;

import org.junit.After;
import org.junit.Before;

public abstract class KVStoreSuite {
  private static KVStore db;

  /**
   * Implementations should override this method; it is called only once, before all tests are
   * run. Any state can be safely stored in static variables and cleaned up in a @AfterClass
   * handler.
   */
  protected abstract KVStore createStore() throws Exception;

  @Before
  public void setup() throws Exception {
    db = createStore();
  }

  @After
  public void tearDown() throws Exception {
    if (db != null) {
      db.close();
      db = null;
    }
  }


}

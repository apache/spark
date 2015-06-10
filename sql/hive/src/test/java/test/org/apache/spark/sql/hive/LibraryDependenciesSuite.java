/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test.org.apache.spark.sql.hive;

import com.google.common.base.Preconditions;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.hive.client.IsolatedClientLoader$;
import org.apache.spark.sql.hive.test.TestHive$;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.Map;

/**
 * This test suite contains some methods simply to force dependencies to load,
 * and so detect transitive version conflicts and binding problems.
 */
public class LibraryDependenciesSuite extends Assert {

  @Test
  public void testLoadGuava() throws Throwable {
    Preconditions.checkArgument(true);
  }

  @Test
  public void testLoadTestHive() throws Throwable {
    assertEquals("1.2.0", HiveContext.hiveExecutionVersion());
  }

  @Test
  public void testContextConfig() throws Throwable {
    Map<String, String> context = HiveContext.newTemporaryConfiguration();
    Iterator<Tuple2<String, String>> iterator = context.iterator();
    while (iterator.hasNext()) {
      Tuple2<String, String> next = iterator.next();
      assertNotNull(next._1(), "null key");
      assertNotNull(next._2(), "null value for key " + next._1());
    }

  }
}

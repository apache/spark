/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.launcher;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * This class used to by {@link SparkLauncher#startApplicationAsThread(SparkAppHandle.Listener...)}
 * <p>
 * This class to start Spark applications programmatically as a Thread. This uses reflection and relies
 * on find SparkSubmit main class.
 * </p>
 */
class SparkSubmitRunner implements Runnable {
  private Method main;
  private final List<String> args;

  SparkSubmitRunner(Method main, List<String> args) {
    this.main = main;
    this.args = args;
  }

  /**
   * It is important that spark-submit class is available in the classpath.
   * Trying to see if "org.apache.spark.deploy.SparkSubmit#Main" method is available in the classpath.
   *
   * @return Method to execute for starting Spark Application
   * @throws ClassNotFoundException
   * @throws NoSuchMethodException
   */
  protected static Method getSparkSubmitMain() throws ClassNotFoundException, NoSuchMethodException {
    Class<?> cls = Class.forName("org.apache.spark.deploy.SparkSubmit");
    return cls.getDeclaredMethod("main", String[].class);
  }

  @Override
  public void run() {
    try {
      if (main == null) {
        main = getSparkSubmitMain();
      }
      Object argsObj = args.toArray(new String[args.size()]);
      main.invoke(null, argsObj);
    } catch (IllegalAccessException illAcEx) {
      throw new RuntimeException(illAcEx);
    } catch (InvocationTargetException invokEx) {
      throw new RuntimeException(invokEx);
    } catch (ClassNotFoundException cnfEx) {
      throw new RuntimeException(cnfEx);
    } catch (NoSuchMethodException nsmEx) {
      throw new RuntimeException(nsmEx);
    }
  }
}

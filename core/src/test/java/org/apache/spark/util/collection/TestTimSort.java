/**
 * Copyright 2015 Stijn de Gouw
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.spark.util.collection;

import java.util.*;

/**
 * This codes generates a int array which fails the standard TimSort.
 *
 * The blog that reported the bug
 * http://www.envisage-project.eu/timsort-specification-and-verification/
 *
 * This codes was originally wrote by Stijn de Gouw, modified by Evan Yu to adapt to
 * our test suite.
 *
 * https://github.com/abstools/java-timsort-bug
 * https://github.com/abstools/java-timsort-bug/blob/master/LICENSE
 */
public class TestTimSort {

  private static final int MIN_MERGE = 32;

  /**
   * Returns an array of integers that demonstrate the bug in TimSort
   */
  public static int[] getTimSortBugTestSet(int length) {
    int minRun = minRunLength(length);
    List<Long> runs = runsJDKWorstCase(minRun, length);
    return createArray(runs, length);
  }

  private static int minRunLength(int n) {
    int r = 0; // Becomes 1 if any 1 bits are shifted off
    while (n >= MIN_MERGE) {
      r |= (n & 1);
      n >>= 1;
    }
    return n + r;
  }

  private static int[] createArray(List<Long> runs, int length) {
    int[] a = new int[length];
    Arrays.fill(a, 0);
    int endRun = -1;
    for (long len : runs) {
      a[endRun += len] = 1;
    }
    a[length - 1] = 0;
    return a;
  }

  /**
   * Fills <code>runs</code> with a sequence of run lengths of the form<br>
   * Y_n     x_{n,1}   x_{n,2}   ... x_{n,l_n} <br>
   * Y_{n-1} x_{n-1,1} x_{n-1,2} ... x_{n-1,l_{n-1}} <br>
   * ... <br>
   * Y_1     x_{1,1}   x_{1,2}   ... x_{1,l_1}<br>
   * The Y_i's are chosen to satisfy the invariant throughout execution,
   * but the x_{i,j}'s are merged (by <code>TimSort.mergeCollapse</code>)
   * into an X_i that violates the invariant.
   *
   * @param length The sum of all run lengths that will be added to <code>runs</code>.
   */
  private static List<Long> runsJDKWorstCase(int minRun, int length) {
    List<Long> runs = new ArrayList<>();

    long runningTotal = 0, Y = minRun + 4, X = minRun;

    while (runningTotal + Y + X <= length) {
      runningTotal += X + Y;
      generateJDKWrongElem(runs, minRun, X);
      runs.add(0, Y);
      // X_{i+1} = Y_i + x_{i,1} + 1, since runs.get(1) = x_{i,1}
      X = Y + runs.get(1) + 1;
      // Y_{i+1} = X_{i+1} + Y_i + 1
      Y += X + 1;
    }

    if (runningTotal + X <= length) {
      runningTotal += X;
      generateJDKWrongElem(runs, minRun, X);
    }

    runs.add(length - runningTotal);
    return runs;
  }

  /**
   * Adds a sequence x_1, ..., x_n of run lengths to <code>runs</code> such that:<br>
   * 1. X = x_1 + ... + x_n <br>
   * 2. x_j >= minRun for all j <br>
   * 3. x_1 + ... + x_{j-2}  <  x_j  <  x_1 + ... + x_{j-1} for all j <br>
   * These conditions guarantee that TimSort merges all x_j's one by one
   * (resulting in X) using only merges on the second-to-last element.
   *
   * @param X The sum of the sequence that should be added to runs.
   */
  private static void generateJDKWrongElem(List<Long> runs, int minRun, long X) {
    for (long newTotal; X >= 2 * minRun + 1; X = newTotal) {
      //Default strategy
      newTotal = X / 2 + 1;
      //Specialized strategies
      if (3 * minRun + 3 <= X && X <= 4 * minRun + 1) {
        // add x_1=MIN+1, x_2=MIN, x_3=X-newTotal  to runs
        newTotal = 2 * minRun + 1;
      } else if (5 * minRun + 5 <= X && X <= 6 * minRun + 5) {
        // add x_1=MIN+1, x_2=MIN, x_3=MIN+2, x_4=X-newTotal  to runs
        newTotal = 3 * minRun + 3;
      } else if (8 * minRun + 9 <= X && X <= 10 * minRun + 9) {
        // add x_1=MIN+1, x_2=MIN, x_3=MIN+2, x_4=2MIN+2, x_5=X-newTotal  to runs
        newTotal = 5 * minRun + 5;
      } else if (13 * minRun + 15 <= X && X <= 16 * minRun + 17) {
        // add x_1=MIN+1, x_2=MIN, x_3=MIN+2, x_4=2MIN+2, x_5=3MIN+4, x_6=X-newTotal to runs
        newTotal = 8 * minRun + 9;
      }
      runs.add(0, X - newTotal);
    }
    runs.add(0, X);
  }
}

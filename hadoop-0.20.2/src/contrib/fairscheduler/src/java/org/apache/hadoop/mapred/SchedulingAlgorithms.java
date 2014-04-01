/**
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

package org.apache.hadoop.mapred;

import java.util.Collection;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility class containing scheduling algorithms used in the fair scheduler.
 */
class SchedulingAlgorithms {
  public static final Log LOG = LogFactory.getLog(
      SchedulingAlgorithms.class.getName());
  
  /**
   * Compare Schedulables in order of priority and then submission time, as in
   * the default FIFO scheduler in Hadoop.
   */
  public static class FifoComparator implements Comparator<Schedulable> {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      int res = s1.getPriority().compareTo(s2.getPriority());
      if (res == 0) {
        res = (int) Math.signum(s1.getStartTime() - s2.getStartTime());
      }
      if (res == 0) {
        // In the rare case where jobs were submitted at the exact same time,
        // compare them by name (which will be the JobID) to get a deterministic
        // ordering, so we don't alternately launch tasks from different jobs.
        res = s1.getName().compareTo(s2.getName());
      }
      return res;
    }
  }

  /**
   * Compare Schedulables via weighted fair sharing. In addition, Schedulables
   * below their min share get priority over those whose min share is met. 
   * 
   * Schedulables below their min share are compared by how far below it they
   * are as a ratio. For example, if job A has 8 out of a min share of 10 tasks
   * and job B has 50 out of a min share of 100, then job B is scheduled next, 
   * because B is at 50% of its min share and A is at 80% of its min share.
   * 
   * Schedulables above their min share are compared by (runningTasks / weight).
   * If all weights are equal, slots are given to the job with the fewest tasks;
   * otherwise, jobs with more weight get proportionally more slots.
   */
  public static class FairShareComparator implements Comparator<Schedulable> {
    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      double minShareRatio1, minShareRatio2;
      double tasksToWeightRatio1, tasksToWeightRatio2;
      int minShare1 = Math.min(s1.getMinShare(), s1.getDemand());
      int minShare2 = Math.min(s2.getMinShare(), s2.getDemand());
      boolean s1Needy = s1.getRunningTasks() < minShare1;
      boolean s2Needy = s2.getRunningTasks() < minShare2;
      minShareRatio1 = s1.getRunningTasks() / Math.max(minShare1, 1.0);
      minShareRatio2 = s2.getRunningTasks() / Math.max(minShare2, 1.0);
      tasksToWeightRatio1 = s1.getRunningTasks() / s1.getWeight();
      tasksToWeightRatio2 = s2.getRunningTasks() / s2.getWeight();
      int res = 0;
      if (s1Needy && !s2Needy)
        res = -1;
      else if (s2Needy && !s1Needy)
        res = 1;
      else if (s1Needy && s2Needy)
        res = (int) Math.signum(minShareRatio1 - minShareRatio2);
      else // Neither schedulable is needy
        res = (int) Math.signum(tasksToWeightRatio1 - tasksToWeightRatio2);
      if (res == 0) {
        // Jobs are tied in fairness ratio. Break the tie by submit time and job 
        // name to get a deterministic ordering, which is useful for unit tests.
        res = (int) Math.signum(s1.getStartTime() - s2.getStartTime());
        if (res == 0)
          res = s1.getName().compareTo(s2.getName());
      }
      return res;
    }
  }

  /** 
   * Number of iterations for the binary search in computeFairShares. This is 
   * equivalent to the number of bits of precision in the output. 25 iterations 
   * gives precision better than 0.1 slots in clusters with one million slots.
   */
  private static final int COMPUTE_FAIR_SHARES_ITERATIONS = 25;
  
  /**
   * Given a set of Schedulables and a number of slots, compute their weighted
   * fair shares. The min shares and demands of the Schedulables are assumed to
   * be set beforehand. We compute the fairest possible allocation of shares 
   * to the Schedulables that respects their min shares and demands.
   * 
   * To understand what this method does, we must first define what weighted
   * fair sharing means in the presence of minimum shares and demands. If there
   * were no minimum shares and every Schedulable had an infinite demand (i.e.
   * could launch infinitely many tasks), then weighted fair sharing would be
   * achieved if the ratio of slotsAssigned / weight was equal for each
   * Schedulable and all slots were assigned. Minimum shares and demands add
   * two further twists:
   * - Some Schedulables may not have enough tasks to fill all their share.
   * - Some Schedulables may have a min share higher than their assigned share.
   * 
   * To deal with these possibilities, we define an assignment of slots as
   * being fair if there exists a ratio R such that:
   * - Schedulables S where S.demand < R * S.weight are assigned share S.demand
   * - Schedulables S where S.minShare > R * S.weight are given share S.minShare
   * - All other Schedulables S are assigned share R * S.weight
   * - The sum of all the shares is totalSlots.
   * 
   * We call R the weight-to-slots ratio because it converts a Schedulable's
   * weight to the number of slots it is assigned.
   * 
   * We compute a fair allocation by finding a suitable weight-to-slot ratio R.
   * To do this, we use binary search. Given a ratio R, we compute the number
   * of slots that would be used in total with this ratio (the sum of the shares
   * computed using the conditions above). If this number of slots is less than
   * totalSlots, then R is too small and more slots could be assigned. If the
   * number of slots is more than totalSlots, then R is too large. 
   * 
   * We begin the binary search with a lower bound on R of 0 (which means that
   * all Schedulables are only given their minShare) and an upper bound computed
   * to be large enough that too many slots are given (by doubling R until we
   * either use more than totalSlots slots or we fulfill all jobs' demands).
   * The helper method slotsUsedWithWeightToSlotRatio computes the total number
   * of slots used with a given value of R.
   * 
   * The running time of this algorithm is linear in the number of Schedulables,
   * because slotsUsedWithWeightToSlotRatio is linear-time and the number of
   * iterations of binary search is a constant (dependent on desired precision).
   */
  public static void computeFairShares(
      Collection<? extends Schedulable> schedulables, double totalSlots) {
    // Find an upper bound on R that we can use in our binary search. We start 
    // at R = 1 and double it until we have either used totalSlots slots or we
    // have met all Schedulables' demands (if total demand < totalSlots).
    double totalDemand = 0;
    for (Schedulable sched: schedulables) {
      totalDemand += sched.getDemand();
    }
    double cap = Math.min(totalDemand, totalSlots);
    double rMax = 1.0;
    while (slotsUsedWithWeightToSlotRatio(rMax, schedulables) < cap) {
      rMax *= 2.0;
    }
    // Perform the binary search for up to COMPUTE_FAIR_SHARES_ITERATIONS steps
    double left = 0;
    double right = rMax;
    for (int i = 0; i < COMPUTE_FAIR_SHARES_ITERATIONS; i++) {
      double mid = (left + right) / 2.0;
      if (slotsUsedWithWeightToSlotRatio(mid, schedulables) < cap) {
        left = mid;
      } else {
        right = mid;
      }
    }
    // Set the fair shares based on the value of R we've converged to
    for (Schedulable sched: schedulables) {
      sched.setFairShare(computeShare(sched, right));
    }
  }
  
  /**
   * Compute the number of slots that would be used given a weight-to-slot
   * ratio w2sRatio, for use in the computeFairShares algorithm as described
   * in #{@link SchedulingAlgorithms#computeFairShares(Collection, double)}.
   */
  private static double slotsUsedWithWeightToSlotRatio(double w2sRatio,
      Collection<? extends Schedulable> schedulables) {
    double slotsTaken = 0;
    for (Schedulable sched: schedulables) {
      double share = computeShare(sched, w2sRatio);
      slotsTaken += share;
    }
    return slotsTaken;
  }

  /**
   * Compute the number of slots assigned to a Schedulable given a particular
   * weight-to-slot ratio w2sRatio, for use in computeFairShares as described
   * in #{@link SchedulingAlgorithms#computeFairShares(Collection, double)}.
   */
  private static double computeShare(Schedulable sched, double w2sRatio) {
    double share = sched.getWeight() * w2sRatio;
    share = Math.max(share, sched.getMinShare());
    share = Math.min(share, sched.getDemand());
    return share;
  }
}

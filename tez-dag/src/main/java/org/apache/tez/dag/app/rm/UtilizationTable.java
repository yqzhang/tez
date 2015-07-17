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

package org.apache.tez.dag.app.rm;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import org.apache.tez.dag.app.rm.UtilizationRecord;
import org.apache.tez.dag.app.rm.UtilizationRecord.JobType;
import org.apache.tez.dag.app.rm.UtilizationRecord.UtilizationRecordType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for storing all classes of utilization records, where each record
 * represents a class of environments that behave similarly as suggested by the
 * clustering algorithm.
 */
public class UtilizationTable {

  private static final Logger LOG = LoggerFactory.getLogger(
      UtilizationTable.class);

  // A generic random generator for probabilistic scheduling
  private Random randomGenerator;
  // The actual records organized as an array of records
  private UtilizationRecord[] utilizationRecords;

  // Whether to probabilistically select types that are not most preferrable.
  // If so, we put a weight on each class to reflect the preference.
  private boolean probabilisticTypeSelection;
  // Weights for the preference sorted from most preferrable to least
  // e.g., [weight(high), weight(medium), weight(low)]
  private double preferenceWeights[];

  // Whether to use best-fit bin packing to pick the class that has the
  // smallest residual capacity after the job placement. This is good for
  // reducing resource fragmentation especially at high utilization.
  private boolean bestFitScheduling;

  /**
   * Constructor that initialize the random generator, and configure the
   * scheduling policy.
   *
   * @param probabilisticTypeSelection Enables scheduling decisions that
   *                                   probabilistically select non-preferrable
   *                                   classes with the following weights
   * @param lowPreferenceWeight Weight for the lowest preference
   * @param mediumPreferenceWeight Weight for the medium preference
   * @param highPreferenceWeight Weight for the highest preference
   * @param bestFitScheduling Enables best-fit bin packing to reduce resource
   *                          fragmentation
   */
  public UtilizationTable(boolean probabilisticTypeSelection,
                          double lowPreferenceWeight,
                          double mediumPreferenceWeight,
                          double highPreferenceWeight,
                          boolean bestFitScheduling) {
    // Seed the random generator by time
    this.randomGenerator = new Random(System.currentTimeMillis());

    // In case we want to do this probabilistically
    this.probabilisticTypeSelection = probabilisticTypeSelection;
    if (probabilisticTypeSelection) {
      this.preferenceWeights = new double[] {
          highPreferenceWeight,
          mediumPreferenceWeight,
          lowPreferenceWeight
      };
    // Enforce the equal weights when probabilistic type selection is disabled
    } else {
      this.preferenceWeights = new double[] {1.0, 1.0, 1.0};
    }

    // In case we want to reduce resource fragmentation by using best-fit
    // bin packing
    this.bestFitScheduling = bestFitScheduling;
  }

  /**
   * TODO: A function that communicates to update all the utilization records.
   */
  public void updateUtilization() { }

  /**
   * Pick class(es) of environments that can best fulfill the resource demand
   * of the given job. There are 3 cases primarily:
   * 1) There is at least one class can fit the entire job
   * 2) There is at least one combination of classes can fit the job
   * 3) There is no combination of classes can fit the job
   * Return the scheduling decision represented in a probabilistic CDF, which
   * is used to avoid lock contention for future scheduling.
   *
   * @param numOfTasks Total number of tasks introduced by the given job
   * @param vcoresPerTask The number of virtual cores required by each task
   * @param memoryPerTask The memory capacity in MB required by each task
   * @param type The type of the job in terms of its runtime duration
   * @return the node labels of the cluster that gets randomly picked
   */
  public ArrayList<Tuple<Double, HashSet<String>>> pickRandomCluster(
                                                       int numOfTasks,
                                                       int vcoresPerTask,
                                                       int memoryPerTask,
                                                       JobType type) {
    UtilizationRecordType[] preferences;

    // A mapping between task duration and the preferences over classes
    switch (type) {
      case T_JOB_SHORT:
        // short jobs: unpredictable > periodic > constant
        preferences = new UtilizationRecordType[] {
            UtilizationRecordType.U_UNPREDICTABLE,
            UtilizationRecordType.U_PERIODIC,
            UtilizationRecordType.U_CONSTANT};
        break;
      case T_JOB_MEDIUM:
        // TODO: not sure whether this is the best, but give periodic some love
        // medium jobs: periodic > constant > unpredictable
        preferences = new UtilizationRecordType[] {
            UtilizationRecordType.U_PERIODIC,
            UtilizationRecordType.U_CONSTANT,
            UtilizationRecordType.U_UNPREDICTABLE};
        break;
      case T_JOB_LONG:
        // long jobs: constant > periodic > unpredictable
        preferences = new UtilizationRecordType[] {
            UtilizationRecordType.U_CONSTANT,
            UtilizationRecordType.U_PERIODIC,
            UtilizationRecordType.U_UNPREDICTABLE};
        break;
      default:
        // what?
        LOG.error("Math is borken");
        return null;
    }

    // Build one list of available containers for each preference (sorted from
    // high to low)
    ArrayList<Tuple<Integer, Integer>>[] availableContainers = new ArrayList[3];
    for (int i = 0; i < 3; i++) {
      availableContainers[i] = new ArrayList<Tuple<Integer, Integer>>();
    }

    for (int i = 0; i < utilizationRecords.length; i++) {
      UtilizationRecord record = utilizationRecords[i];
      // Maximum number of containers can fit in
      int numOfContainers = record.floor(vcoresPerTask, memoryPerTask, type);

      // High preference
      if (record.getType() == preferences[0]) {
        availableContainers[0].add(
            new Tuple<Integer, Integer>(numOfContainers, i));
      // Medium preference
      } else if (record.getType() == preferences[1]) {
        availableContainers[1].add(
            new Tuple<Integer, Integer>(numOfContainers, i));
      // Low preference
      } else if (record.getType() == preferences[2]) {
        availableContainers[2].add(
            new Tuple<Integer, Integer>(numOfContainers, i));
      // What?
      } else {
        LOG.error("Math is borken");
        return null;
      }
    }

    /**
     * 1) There is at least one class can fit the entire job.
     *   a. if probablistic type selection is enabled, we pick one class based
     *      on the product of its capacity (i.e., headroom in terms of number
     *      of containers) and the weights representing our preference.
     *   b. if probablistic type selection is disabled, we pick one class as
     *      soon as we can find at least one class from the most preferrable
     *      classes (e.g., if we find 2 classes in the high preference list, we
     *      will pick one out of the two without looking at medium and low
     *      preference list).
     *
     * Note that the probability we pick each class will be proportional to its
     * headroom when best-fit scheduling is disabled, and inversely proportional
     * to the headroom when it is enabled.
     */
    // List of classes that can fit the job, sorted from highest preference to
    // lowest
    ArrayList<Tuple<Double, Integer>> fittedList =
        new ArrayList<Tuple<Double, Integer>>();
    double cumulativeNumOfContainers = 0.0;

    // Loop through preferences
    for (int i = 0; i < 3; i++) {

      // Loop through classes have the same preference
      for (int j = 0; j < availableContainers[i].size(); j++) {
        int numOfAvailableContainers = availableContainers[i].get(j).getFirst();
        int indexInRecords = availableContainers[i].get(j).getSecond();

        if (numOfAvailableContainers >= numOfTasks) {
          // Put the weighted tuple into the list, this works even if we are not
          // doing probabilistic type selection since we are just scaling things
          // with the same weight
          double weightedNumOfContainers =
              numOfAvailableContainers * preferenceWeights[i];
          Tuple<Double, Integer> weightedTuple =
              new Tuple<Double, Integer>(weightedNumOfContainers,
                                         indexInRecords);
          fittedList.add(weightedTuple);
          cumulativeNumOfContainers += weightedNumOfContainers;
        }
      }

      /**
       * Make scheduling decision if we have find at least one fit and either
       * one of the following condition is true.
       *  a. we are not doing probabilistic type selection
       *  b. we have already iterated through all types
       */
      if (cumulativeNumOfContainers > 0.0 &&
          ((!probabilisticTypeSelection) || i == 2)) {
        int indexInList =
            selectSingleRecord(fittedList, cumulativeNumOfContainers);
        int indexInRecords = fittedList.get(indexInList).getSecond();

        // Construct the scheduling decision, which only picks from one class
        ArrayList<Tuple<Double, HashSet<String>>> scheduleList =
            new ArrayList<Tuple<Double, HashSet<String>>>();
        Tuple<Double, HashSet<String>> scheduleTuple =
            new Tuple<Double, HashSet<String>>(
                    1.0,
                    utilizationRecords[indexInRecords].getClusterNodeLabels());
        scheduleList.add(scheduleTuple);
        return scheduleList;
      }
    }

    /**
     * 2) There is at least one combination of classes can fit the job.
     *   a. if probabilistic type selection is enabled, we pick from all classes
     *      based on the product of its capacity (i.e., headroom in terms of
     *      number of containers) and the weights representing our preference.
     *   b. if probabilistic type selection is disabled, we pick from a subset
     *      of classes as soon as we can find at least one combination from the
     *      most preferrable classes (e.g., if we have found 2 classes in high
     *      and medium preference list that can fit the job in combination, we
     *      will not keep looking at low preference list).
     *
     * Note that the best-fit scheduling does not have any impact here.
     */
    // A list to keep track of the actual number of available containers
    ArrayList<Tuple<Integer, Integer>> actualList =
        new ArrayList<Tuple<Integer, Integer>>();
    // A list to keep track of the weighted number of available containers
    ArrayList<Tuple<Double, Integer>> weightedList =
        new ArrayList<Tuple<Double, Integer>>();
    double actualCumulativeNumOfContainers = 0.0;
    double weightedCumulativeNumOfContainers = 0.0;

    // Loop through preferences
    for (int i = 0; i < 3; i++) {

      // Loop through classes have the same preference
      for (int j = 0; j < availableContainers[i].size(); j++) {
        int actualNumOfContainers = availableContainers[i].get(j).getFirst();
        double weightedNumOfContainers =
            actualNumOfContainers * preferenceWeights[j];
        int indexInRecords = availableContainers[i].get(j).getSecond();

        // actual number of containers
        Tuple<Integer, Integer> actualTuple =
            new Tuple<Integer, Integer>(actualNumOfContinaers,
                                        indexInRecords);
        actualList.add(actualTuple);
        actualCumulativeNumOfContainers += actualNumOfContainers;

        // weighted number of containers
        Tuple<Double, Integer> weightedTuple =
            new Tuple<Double, Integer>(weightedNumOfContainers,
                                       indexInRecords);
        weightedList.add(weightedTuple);
        weightedCumulativeNumOfContainers += weightedNumOfContainers;
      }

      /**
       * Make scheduling decisions if we have at least one combination and
       * either one of the following condition is true.
       *  a. we are not doing probabilistic type selection
       *  b. we have already iterated through all the types
       */
      if (actualCumulativeNumOfContainers >= numOfTasks &&
          (!probabilisticTypeSelection || i == 2)) {

        ArrayList<Tuple<Double, HashSet<String>>> scheduleList =
            new ArrayList<Tuple<Double, HashSet<String>>>();
        // Number of containers we have already scheduled
        int scheduledActualNumOfContainers = 0;

        while (scheduledActualNumOfContainers < numOfTasks) {
          // Randomly pick one class based on weighted number of containers
          int indexInList = selectSingleRecord(
                                weightedList,
                                weightedCumulativeNumOfContainers);
          // Index in the original records
          int indexInRecords = actualList.get(indexInList).getSecond();
          // The acutal number of containers the class has
          int actualNumOfContainers = actualList.get(indexInList).getFirst();
          // The weighted number of containers the class has
          double weightedNumOfContainers =
              weightedList.get(indexInList).getFirst();
          // Aggregated the number of containers we have scheduled
          scheduledActualNumOfContainers += actualNumOfContainers;
          // Calculate the probability in CDF for this class
          double scheduledCDF =
              (double) scheduledActualNumOfContainers / numOfTasks;
          // We have more than needed
          if (scheduledCDF > 1.0) {
            scheduledCDF = 1.0;
            assert scheduledActualNumOfContainers >= numOfTasks;
          }

          // Create the tuple
          Tuple<Double, HashSet<String>> scheduleTuple =
              new Tuple<Double, HashSet<String>>(
                    scheduledCDF,
                    utilizationRecords[indexInRecords].getClusterNodeLabels());
          scheduleList.add(scheduleTuple);

          // Update the actual and weighted lists
          weightedCumulativeNumOfContainers -= weightedNumOfContainers;
          actualList.remove(indexInList);
          weightedList.remove(indexInList);
        }

        return scheduleList;
      /**
       * Make scheduling decisions if we have iterated through all the classes
       * but still cannot find even one combination that fits the job. In this
       * case, we just pick one class probabilistically and queue for it.
       */
      } else if (i == 2) {
        ArrayList<Tuple<Double, HashSet<String>>> scheduleList =
            new ArrayList<Tuple<Double, HashSet<String>>>();

        // Wait for one single class based on probability
        int indexInList = selectSingleRecord(
                              weightedList,
                              weightedCumulativeNumOfContainers);
        int indexInRecords = actualList.get(indexInList).getSecond();
        Tuple<Double, HashSet<String>> scheduleTuple =
            new Tuple<Double, HashSet<String>>(
                    1.0,
                    utilizationRecords[indexInRecords].getClusterNodeLabels());
        scheduleList.add(scheduleTuple);

        return scheduleList;
      } else {
        LOG.error("Math is broken");
        return null;
      }
    }

    LOG.error("Math is broken again");
    return null;
  }

  /**
   * Randomly select one element in the list based on probability.
   *
   * @param containerList The list of classes
   * @param cumulativeNumOfContainers Total number of containers in the list
   * @return The index of selected element in the ArrayList
   */
  private int selectSingleRecord(
                  ArrayList<Tuple<Double, Integer>> containerList,
                  double cumulativeNumOfContainers) {
    // Build a CDF based on the numbers of available containers
    double[] containerCDF = new double[containerList.size()];
    containerCDF[0] =
      containerList.get(0).getFirst() / cumulativeNumOfContainers;
    for (int i = 1; i < containerList.size(); i++) {
      containerCDF[i] = containerCDF[i - 1] +
          containerList.get(i).getFirst() / cumulativeNumOfContainers;
    }
    // Since it is an CDF
    assert containerCDF[containerCDF.length - 1] == 1.0;

    // Randomly pick one
    double rand = this.randomGenerator.nextDouble();
    return lowerBound(containerCDF, rand);
  }

  /**
   * Find the index of the first element that is greater than or equal to the
   * target value given a sorted array.
   *
   * @param array An array sorted from smallest to largest
   * @param target The target value
   * @return The index of the first element in the array that is greater than
   *         or equal to the target value
   */
  public static int lowerBound(double[] array, double target) {
    int low = 0;
    int high = array.length;

    while (low != high) {
      int mid = (low + high) / 2;
      if (array[mid] <= target) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }

    return high;
  }

  /**
   * A class for storing tuples
   */
  public class Tuple<X, Y> {
    
    private X first;
    private Y second;

    public Tuple(X first, Y second) {
      this.first = first;
      this.second = second;
    }

    public X getFirst() {
      return this.first;
    }

    public Y getSecond() {
      return this.second;
    }
  }
}

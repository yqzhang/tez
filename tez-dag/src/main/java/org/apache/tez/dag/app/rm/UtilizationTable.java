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
import org.apache.tez.dag.app.rm.UtilizationRecord.TaskType;
import org.apache.tez.dag.app.rm.UtilizationRecord.UtilizationRecordType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for the data structure to store the utilization data, in which each
 * record represents a cluster of environments that behave similarly as
 * suggested by the clustering algorithm.
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
  // Weights for preference (from high to low)
  private double preferenceWeights[];

  // Whether to use best-fit bin packing pick the class that has the smallest
  // residual capacity, which could potentially reduce resource fragmentation
  private boolean bestFitScheduling;

  /**
   * Constructor that initialize the random generator, and configure the
   * scheduling policy.
   *
   * @param probabilisticTypeSelection Enables scheduling decisions that
   *                                   probabilistically select non-preferrable
   *                                   classes by the following weights
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
   * Randomly pick a cluster of environments that can fulfill the resource
   * demand, and the probability of picking each cluster is proportional to the
   * amount of available resources.
   *
   * TODO:
   * @param resourceNeeded The amount of CPU resources needed by the task(s) in
   *                       double ranging in (0.0, 1.0]
   * @return the node labels of the cluster that gets randomly picked
   */
  public ArrayList<Tuple<Double, HashSet<String>>> pickRandomCluster(
                                                       int numOfTasks,
                                                       int vcoresPerTask,
                                                       int memoryPerTask,
                                                       TaskType type) {
    UtilizationRecordType[] preferences;

    // A mapping between task duration and the preferences over classes
    switch (type) {
      case T_SHORT:
        // unpredictable > periodic > constant
        preferences = new UtilizationRecordType[] {
            UtilizationRecordType.U_UNPREDICTABLE,
            UtilizationRecordType.U_PERIODIC,
            UtilizationRecordType.U_CONSTANT};
        break;
      case T_MEDIUM:
        // TODO: not sure whether this is the best, but give periodic some love
        // periodic > constant > unpredictable
        preferences = new UtilizationRecordType[] {
            UtilizationRecordType.U_PERIODIC,
            UtilizationRecordType.U_CONSTANT,
            UtilizationRecordType.U_UNPREDICTABLE};
        break;
      case T_LONG:
        // constant > periodic > unpredictable
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

    // Build the list of available containers
    ArrayList<Tuple<Integer, Integer>>[] availableContainers = new ArrayList[3];
    for (int i = 0; i < 3; i++) {
      availableContainers[i] = new ArrayList<Tuple<Integer, Integer>>();
    }

    for (int i = 0; i < utilizationRecords.length; i++) {
      UtilizationRecord record = utilizationRecords[i];
      int numOfContainers = record.floor(vcoresPerTask, memoryPerTask, type);
      if (record.getType() == preferences[0]) {
        availableContainers[0].add(new Tuple<Integer, Integer>(numOfContainers, i));
      } else if (record.getType() == preferences[1]) {
        availableContainers[1].add(new Tuple<Integer, Integer>(numOfContainers, i));
      } else if (record.getType() == preferences[2]) {
        availableContainers[2].add(new Tuple<Integer, Integer>(numOfContainers, i));
      } else {
        // what?
        LOG.error("Math is borken");
        return null;
      }
    }

    // 1. single class fits all
    ArrayList<Tuple<Double, Integer>> fittedList =
        new ArrayList<Tuple<Double, Integer>>();
    double cumulativeNumOfContainers = 0.0;

    // Loop through preferences
    for (int i = 0; i < 3; i++) {

      // Loop through classes
      for (int j = 0; j < availableContainers[i].size(); j++) {
        int numOfAvailableContainers = availableContainers[i].get(j).getFirst();
        int indexInRecords = availableContainers[i].get(j).getSecond();

        if (numOfAvailableContainers >= numOfTasks) {
          // Put the weighted tuple into the list, this works even if we are not
          // doing probabilistic type selection since we are just scaling things
          // with the same weight
          double weightedNumOfContainers =
              numOfAvailableContainers * preferenceWeights[j];
          Tuple<Double, Integer> weightedTuple =
              new Tuple<Double, Integer>(weightedNumOfContainers, indexInRecords);
          fittedList.add(weightedTuple);
          cumulativeNumOfContainers += weightedNumOfContainers;
        }
      }

      if (cumulativeNumOfContainers > 0.0 &&
          ((!probabilisticTypeSelection) || i == 2)) {
        int indexInList =
            selectSingleRecord(fittedList, cumulativeNumOfContainers);
        int indexInRecords = fittedList.get(indexInList).getSecond();

        ArrayList<Tuple<Double, HashSet<String>>> scheduleList =
            new ArrayList<Tuple<Double, HashSet<String>>>();
        Tuple<Double, HashSet<String>> scheduleAllTuple =
            new Tuple<Double, HashSet<String>>(
                    1.0,
                    utilizationRecords[indexInRecords].getClusterNodeLabels());
        scheduleList.add(scheduleAllTuple);
        return scheduleList;
      }
    }

    // 2. multiple classes are needed
    // Actual number of containers
    ArrayList<Tuple<Integer, Integer>> actualList =
        new ArrayList<Tuple<Integer, Integer>>();
    // Weighted number of containers
    ArrayList<Tuple<Double, Integer>> weightedList =
        new ArrayList<Tuple<Double, Integer>>();
    double actualCumulativeNumOfContainers = 0.0;
    double weightedCumulativeNumOfContainers = 0.0;

    // Loop through preferences
    for (int i = 0; i < 3; i++) {

      // Loop through classes
      for (int j = 0; j < availableContainers[i].size(); j++) {
        int numOfAvailableContainers = availableContainers[i].get(j).getFirst();
        int indexInRecords = availableContainers[i].get(j).getSecond();
        double weightedNumOfContainers =
            numOfAvailableContainers * preferenceWeights[j];

        // actual number of containers
        Tuple<Integer, Integer> actualTuple =
            new Tuple<Integer, Integer>(numOfAvailableContainers,
                                        indexInRecords);
        actualList.add(actualTuple);
        actualCumulativeNumOfContainers += numOfAvailableContainers;

        // weighted number of containers
        Tuple<Double, Integer> weightedTuple =
            new Tuple<Double, Integer>(weightedNumOfContainers,
                                       indexInRecords);
        weightedList.add(weightedTuple);
        weightedCumulativeNumOfContainers += weightedNumOfContainers;
      }

      // Make scheduling decisions if we have enough containers and:
      // 1) not probabilistic type selection
      // 2) we have gone through all the preferences
      if (actualCumulativeNumOfContainers >= numOfTasks &&
          (!probabilisticTypeSelection || i == 2)) {

        ArrayList<Tuple<Double, HashSet<String>>> scheduleList =
            new ArrayList<Tuple<Double, HashSet<String>>>();
        // Number of containers we have already scheduled
        double scheduledActualNumOfContainers = 0.0;

        int numOfTasksToSchedule = numOfTasks;
        while (numOfTasksToSchedule > 0) {
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
          double scheduledCDF = scheduledActualNumOfContainers / numOfTasks;
          scheduledCDF = (scheduledCDF > 1.0) ? 1.0 : scheduledCDF;

          // Create the tuple
          Tuple<Double, HashSet<String>> scheduleTuple =
              new Tuple<Double, HashSet<String>>(
                    scheduledCDF,
                    utilizationRecords[indexInRecords].getClusterNodeLabels());
          scheduleList.add(scheduleTuple);

          // Update the actual and weighted lists
          numOfTasksToSchedule -= actualNumOfContainers;
          weightedCumulativeNumOfContainers -= weightedNumOfContainers;
          actualList.remove(indexInList);
          weightedList.remove(indexInList);
        }

        return scheduleList;
      // Make scheduling decisions if we are running out of containers
      } else if (actualCumulativeNumOfContainers < numOfTasks && i == 2) {
        ArrayList<Tuple<Double, HashSet<String>>> scheduleList =
            new ArrayList<Tuple<Double, HashSet<String>>>();

        // Wait for a single class purely based on probability
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

    LOG.error("Math is broken");
    return null;
  }

  /**
   * Randomly select one element in the list based on probability.
   *
   * TODO:
   * @param containerList The list of classes
   * @param cumulativeNumOfContainers Total number of containers in the list
   * @return The index of selected element in the ArrayList
   */
  private int selectSingleRecord(ArrayList<Tuple<Double, Integer>> containerList,
                                 double cumulativeNumOfContainers) {
    // Build a CDF based on the numbers of available containers
    double[] containerCDF = new double[containerList.size()];
    containerCDF[0] = containerList.get(0).getFirst() / cumulativeNumOfContainers;
    for (int i = 1; i < containerList.size(); i++) {
      containerCDF[i] = containerCDF[i - 1] +
          containerList.get(i).getFirst() / cumulativeNumOfContainers;
    }

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

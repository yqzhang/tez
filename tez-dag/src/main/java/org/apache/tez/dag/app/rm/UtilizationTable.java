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

import java.util.HashSet;
import java.util.Random;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for the data structure to store the utilization data, in which each
 * record represents a cluster of environments that behave similarly as
 * suggested by the clustering algorithm.
 */
public class UtilizationTable {

  // A generic random generator for probabilistic scheduling
  private Random randomGenerator;
  // The actual records organized as an array of records
  private UtilizationRecord[] utilizationRecords;

  /**
   * A simple constructor, that the only thing it needs to do is to seed the
   * random generator with the current system time.
   */
  public UtilizationTable() {
    this.randomGenerator = new Random(System.currentTimeMillis());
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
   * @param resourceNeeded The amount of CPU resources needed by the task(s) in
   *                       double ranging in (0.0, 1.0]
   * @return the node labels of the cluster that gets randomly picked
   */
  public HashSet<String> pickRandomCluster(double resourceNeeded) {
    double[] availableResourceCDF = new double[utilizationRecords.length];

    // Build a CDF of available resources
    double cumulativeResource = 0.0;
    for (int i = 0; i < this.utilizationRecords.length; i++) {
      cumulativeResource +=
          (this.utilizationRecords[i].getMaxUtilization() <=
           1.0 - resourceNeeded) ?
          1.0 - this.utilizationRecords[i].getAvgUtilization() : 0.0;
      availableResourceCDF[i] = cumulativeResource;
    }

    // The probability of picking a given cluster is proportional to
    // the amount of available resources it provides
    for (int i = 0; i < availableResourceCDF.length; i++) {
      availableResourceCDF[i] /= cumulativeResource;
    }

    // Randomly pick one cluster
    double rand = this.randomGenerator.nextDouble();
    int clusterIndex = lowerBound(availableResourceCDF, rand);

    return
        this.utilizationRecords[clusterIndex].getClusterNodeLabels();
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
  private static int lowerBound(double[] array, double target) {
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
   * A class for the data structure to store individual utilization record for
   * one cluster of environments.
   */
  private class UtilizationRecord {

    // all the node labels in the cluster
    private HashSet<String> clusterNodeLabels;
    // The maximum utilization of the cluster represented in double [0.0, 1.0]
    private double maxUtilization;
    // The average utilization of the cluster represented in double [0.0, 1.0]
    private double avgUtilization;
    // Save this for the full utilization trace
    private TreeMap<Integer, Double> utilizationTrace;

    /**
     * A constructor that does not require the full trace of utilization.
     *
     * @param clusterNodeLabels A set of node labels that includes all the
     *                          environments in the cluster
     * @param maxUtilization The maximum utilization of the cluster
     * @param avgUtilization The average utilization of the cluster
     */
    protected UtilizationRecord(HashSet<String> clusterNodeLabels,
                                double maxUtilization,
                                double avgUtilization) {
      this.clusterNodeLabels = clusterNodeLabels;
      this.maxUtilization = maxUtilization;
      this.avgUtilization = avgUtilization;
    }

    /**
     * A constructor that requires the full trace of utilization.
     *
     * @param clusterNodeLabels A set of node labels that includes all the
     *                          environments in the cluster
     * @param maxUtilization The maximum utilization of the cluster
     * @param avgUtilization The average utilization of the cluster
     * @param utilizationTrace The full utilization trace of the cluster center
     *                         represented by a time series
     */
    protected UtilizationRecord(HashSet<String> clusterNodeLabels,
                                double maxUtilization,
                                double avgUtilization,
                                TreeMap<Integer, Double> utilizationTrace) {
      this(clusterNodeLabels, maxUtilization, avgUtilization);
      this.utilizationTrace = utilizationTrace;
    }

    /**
     * Getter function for the node labels.
     *
     * @return The node labels
     */
    protected HashSet<String> getClusterNodeLabels() {
      return this.clusterNodeLabels;
    }
    
    /**
     * Setter function for the node labels.
     *
     * @param clusterNodeLabels The node labels to set
     */
    protected void setClusterNodeLabels(HashSet<String> clusterNodeLabels) {
      this.clusterNodeLabels = clusterNodeLabels;
    }

    /**
     * Getter function for the maximum utilization.
     *
     * @return The maximum utilization
     */
    protected double getMaxUtilization() {
      return this.maxUtilization;
    }

    /**
     * Setter function for the maximum utilization.
     *
     * @param maxUtilization The maximum utilization
     */
    protected void setMaxUtilization(double maxUtilization) {
      this.maxUtilization = maxUtilization;
    }

    /**
     * Getter function for the average utilization.
     *
     * @return The average utilization
     */
    protected double getAvgUtilization() {
      return this.avgUtilization;
    }

    /**
     * Setter function for the average utilization.
     *
     * @param avgUtilization The average utilization
     */
    protected void setAvgUtilization(double avgUtilization) {
      this.avgUtilization = avgUtilization;
    }

    /**
     * Getter function for the full utilization trace.
     *
     * @return The full utilization trace
     */
    protected TreeMap<Integer, Double> getUtilizationTrace() {
      return this.utilizationTrace;
    }

    /**
     * Setter function for the full utilization trace.
     *
     * @param utilizationTrace The full utilization trace
     */
    protected void setUtilizationTrace(
                       TreeMap<Integer, Double> utilizationTrace) {
      this.utilizationTrace = utilizationTrace;
    }
  }
}

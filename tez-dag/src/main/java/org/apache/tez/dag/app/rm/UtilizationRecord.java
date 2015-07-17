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

import java.lang.Math;
import java.util.HashSet;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for the data structure to store individual utilization record for
 * one class of environments.
 */
public class UtilizationRecord {

  private static final Logger LOG = LoggerFactory.getLogger(
      UtilizationRecord.class);

  // Type of the utilization record in {periodic, constant, unpredictable}
  private final UtilizationRecordType type;
  // All the node labels in the class represented in set
  private HashSet<String> clusterNodeLabels;

  // Aggregated virtual core headroom for short-running jobs
  private final double shortVcoresHeadroom;
  // Aggregated virtual core headroom for medium-running jobs
  private final double mediumVcoresHeadroom;
  // Aggregated virtual core headroom for long-running jobs
  private final double longVcoresHeadroom;
  // Aggregated memory headroom in MB
  private final long memoryHeadroom;

  // The full utilization trace
  private TreeMap<Integer, Double> utilizationTrace;

  /**
   * Type of each utilization record suggested by the clustering algorithm.
   */
  enum UtilizationRecordType {
    U_PERIODIC,
    U_CONSTANT,
    U_UNPREDICTABLE
  }

  /**
   * Type of each DAG by its duration
   */
  enum TaskType {
    T_SHORT,
    T_MEDIUM,
    T_LONG
  }

  /**
   * Constructor that directly takes pre-computed headrooms.
   *
   * @param type Type of the given utilization record
   * @param clusterNodeLabels All the node labels of the given record
   * @param shortVcoresHeadroom Headroom in virtual cores for short jobs
   * @param mediumVcoresHeadroom Headroom in virtual cores for medium jobs
   * @param longVcoresHeadroom Headroom in virtual cores for long jobs
   * @param memoryHeadroom Memory headroom in MB
   */
  public UtilizationRecord(UtilizationRecordType type,
                           HashSet<String> clusterNodeLabels,
                           double shortVcoresHeadroom,
                           double mediumVcoresHeadroom,
                           double longVcoresHeadroom,
                           long memoryHeadroom) {
    this.type = type;
    this.clusterNodeLabels = clusterNodeLabels;
    this.shortVcoresHeadroom = shortVcoresHeadroom;
    this.mediumVcoresHeadroom = mediumVcoresHeadroom;
    this.longVcoresHeadroom = longVcoresHeadroom;
    this.memoryHeadroom = memoryHeadroom;
  }

  /**
   * Constructor that directly takes pre-computed headrooms, as well as
   * the full utilization trace.
   *
   * @param type Type of the given utilization record
   * @param clusterNodeLabels All the node labels of the given record
   * @param shortVcoresHeadroom Headroom in virtual cores for short jobs
   * @param mediumVcoresHeadroom Headroom in virtual cores for medium jobs
   * @param longVcoresHeadroom Headroom in virtual cores for long jobs
   * @param memoryHeadroom Memory headroom in MB
   * @param utilizationTrace The full utilization trace of the given record
   */
  public UtilizationRecord(UtilizationRecordType type,
                           HashSet<String> clusterNodeLabels,
                           double shortVcoresHeadroom,
                           double mediumVcoresHeadroom,
                           double longVcoresHeadroom,
                           long memoryHeadroom,
                           TreeMap<Integer, Double> utilizationTrace) {
    this(type, clusterNodeLabels, shortVcoresHeadroom, mediumVcoresHeadroom,
         longVcoresHeadroom, memoryHeadroom);
    this.utilizationTrace = utilizationTrace;
  }

  /**
   * Constructor that takes the raw data and computes the headrooms.
   *
   * @param type Type of the given utilization record
   * @param clusterNodeLabels All the node labels of the given record
   * @param primaryUsedVcores The amount of virtual CPU primary tenants are
   *                          using currently
   * @param primaryUsedVcoresCeiled The ceiling of the virtual CPU usage by
   *                                primary tenants
   * @param secondaryUsedVcores The number of virtual CPU used by secondary
   *                            tenants currently
   * @param availableVcores Number of available virtual CPUs
   * @param primaryUsedMemoryInMB Memory used by primary tenants currently
   * @param secondaryUsedMemoryInMB Memory used by secondary tenants currently
   * @param availableMemoryInMB Amount of available memory
   * @param maxUtilization The maximum utilization of the class
   * @param avgUtilization The average utilization of the class
   */
  public UtilizationRecord(UtilizationRecordType type,
                           HashSet<String> clusterNodeLabels,
                           double primaryUsedVcores,
                           long primaryUsedVcoresCeiled,
                           long secondaryUsedVcores,
                           long availableVcores,
                           long primaryUsedMemoryInMB,
                           long secondaryUsedMemoryInMB,
                           long availableMemoryInMB,
                           double maxUtilization,
                           double avgUtilization) {
    this.type = type;
    this.clusterNodeLabels = clusterNodeLabels;

    double vcoresCapacity = 
        primaryUsedVcoresCeiled + secondaryUsedVcores + availableVcores;
    // headroom_short = capacity - Current
    this.shortVcoresHeadroom = vcoresCapacity *
        (1.0 - (primaryUsedVcores + secondaryUsedVcores) / vcoresCapacity);
    // headroom_medium = capacity - MAX(AVG, Current)
    this.mediumVcoresHeadroom = vcoresCapacity *
        (1.0 - Math.max(avgUtilization,
                        (primaryUsedVcores + secondaryUsedVcores) / vcoresCapacity));
    // headroom_long = capacity - MAX(PEAK, Current)
    this.longVcoresHeadroom = vcoresCapacity *
        (1.0 - Math.max(maxUtilization,
                        (primaryUsedVcores + secondaryUsedVcores) / vcoresCapacity));
    // headroom = current available memory (TODO: memory does not grow)
    this.memoryHeadroom = availableMemoryInMB;
  }

  /**
   * Constructor that takes the raw data and computes the headrooms, which also
   * requires the full trace of the utilization.
   *
   * @param type Type of the given utilization record
   * @param clusterNodeLabels All the node labels of the given record
   * @param primaryUsedVcores The amount of virtual CPU primary tenants are
   *                          using currently
   * @param primaryUsedVcoresCeiled The ceiling of the virtual CPU usage by
   *                                primary tenants
   * @param secondaryUsedVcores The number of virtual CPU used by secondary
   *                            tenants currently
   * @param availableVcores Number of available virtual CPUs
   * @param primaryUsedMemoryInMB Memory used by primary tenants currently
   * @param secondaryUsedMemoryInMB Memory used by secondary tenants currently
   * @param availableMemoryInMB Amount of available memory
   * @param maxUtilization The maximum utilization of the class
   * @param avgUtilization The average utilization of the class
   * @param utilizationTrace The full utilization trace
   */
  public UtilizationRecord(UtilizationRecordType type,
                           HashSet<String> clusterNodeLabels,
                           double primaryUsedVcores,
                           long primaryUsedVcoresCeiled,
                           long secondaryUsedVcores,
                           long availableVcores,
                           long primaryUsedMemoryInMB,
                           long secondaryUsedMemoryInMB,
                           long availableMemoryInMB,
                           double maxUtilization,
                           double avgUtilization,
                           TreeMap<Integer, Double> utilizationTrace) {
    this(type, clusterNodeLabels, primaryUsedVcores, primaryUsedVcoresCeiled,
         secondaryUsedVcores, availableVcores, primaryUsedMemoryInMB,
         secondaryUsedMemoryInMB, availableMemoryInMB, maxUtilization,
         avgUtilization);
    this.utilizationTrace = utilizationTrace;
  }

  /**
   * Get the type of the record in {periodic, constant, unpredictable}.
   *
   * @return The type of the utilization record
   */
  public UtilizationRecordType getType() {
    return this.type;
  }

  /**
   * Getter function for the node labels.
   *
   * @return The node labels
   */
  public HashSet<String> getClusterNodeLabels() {
    return this.clusterNodeLabels;
  }
  
  /**
   * Get the amount of virtual CPU headroom.
   *
   * @param type The type of the task in terms of its duration
   * @return The amount virtual CPU headroom
   */
  public double getVcoresHeadroom(TaskType type) {
    switch(type) {
      case T_SHORT:
        return this.shortVcoresHeadroom;
      case T_MEDIUM:
        return this.mediumVcoresHeadroom;
      case T_LONG:
        return this.longVcoresHeadroom;
      default:
        LOG.error("Math is borken.");
        return 0.0;
    }
  }

  /**
   * Get the amount of memory headroom.
   *
   * @return The amount of memory headroom in MB
   */
  public long getMemoryHeadroom() {
    return this.memoryHeadroom;
  }

  /**
   * Getter function for the full utilization trace.
   *
   * @return The full utilization trace
   */
  public TreeMap<Integer, Double> getUtilizationTrace() {
    return this.utilizationTrace;
  }

  /**
   * Check whether a given utilization class has enough headroom to execute a
   * DAG.
   *
   * @param requiredVcores The amount of virtual CPU required by the DAG
   * @param requiredMemory The amount of memory required by the DAG
   * @param type The type of the task in terms of its duration
   * @return True if the DAG fits, otherwise false
   */
  public boolean fits(long requiredVcores, long requiredMemory, TaskType type) {
    double vcoresHeadroom = getVcoresHeadroom(type);
    return ((requiredVcores <= vcoresHeadroom) &&
            (requiredMemory <= this.memoryHeadroom));
  }

  /**
   * Calculate the maximum number of tasks that can fit in the remaining headroom.
   *
   * @param vcoresPerTask Number of virtual cores required by one single task
   * @param memoryPerTask Amount of memory required by one single task
   * @param type The type of the task in terms of its duration
   * @return The maximum number of tasks that can fit in
   */
  public int floor(int vcoresPerTask, int memoryPerTask, TaskType type) {
    double vcoresHeadroom = getVcoresHeadroom(type);
    int floorByVcores = (int) Math.floor(vcoresHeadroom / vcoresPerTask);
    int floorByMemory = (int) getMemoryHeadroom() / memoryPerTask;

    return Math.min(floorByVcores, floorByMemory);
  }
}

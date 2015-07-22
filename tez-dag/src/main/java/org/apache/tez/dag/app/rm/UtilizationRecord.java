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
 * A class for storing individual utilization record for each class of
 * environments.
 */
public class UtilizationRecord {

  private static final Logger LOG = LoggerFactory.getLogger(
      UtilizationRecord.class);

  // Class type of the environments in {periodic, constant, unpredictable}
  private final UtilizationRecordType type;
  // All node labels in the class represented in set
  private HashSet<String> clusterNodeLabels;
  // The full utilization trace
  private TreeMap<Integer, Double> utilizationTrace;

  // Aggregated virtual core headroom by class for short-running jobs
  private final double shortVcoresHeadroom;
  // Aggregated virtual core headroom by class for medium-running jobs
  private final double mediumVcoresHeadroom;
  // Aggregated virtual core headroom by class for long-running jobs
  private final double longVcoresHeadroom;
  // Aggregated memory headroom by class in MB
  private final long memoryHeadroom;

  /**
   * Type of each utilization record suggested by the clustering algorithm
   * in {periodic, constant, unpredictable}.
   */
  public enum UtilizationRecordType {
    U_PERIODIC,
    U_CONSTANT,
    U_UNPREDICTABLE
  }

  /**
   * Type of the job in terms of runtime duration in {short, medium, long},
   * which determines our preference of environemnts.
   */
  public enum JobType {
    T_JOB_SHORT,
    T_JOB_MEDIUM,
    T_JOB_LONG
  }

  /**
   * Constructor that directly takes pre-computed headrooms.
   *
   * @param type Type of the given class of environments
   * @param clusterNodeLabels All the node labels in the class of environments
   * @param shortVcoresHeadroom Headroom in number of virtual cores for
   *                            short-running jobs
   * @param mediumVcoresHeadroom Headroom in number of virtual cores for
   *                             medium-running jobs
   * @param longVcoresHeadroom Headroom in number of virtual cores for
   *                           long-running jobs
   * @param memoryHeadroom Memory capacity headroom in MB
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
   * @param type Type of the given class of environments
   * @param clusterNodeLabels All the node labels in the class of environments
   * @param shortVcoresHeadroom Headroom in number of virtual cores for
   *                            short-running jobs
   * @param mediumVcoresHeadroom Headroom in number of virtual cores for
   *                             medium-running jobs
   * @param longVcoresHeadroom Headroom in number of virtual cores for
   *                           long-running jobs
   * @param memoryHeadroom Memory capacity headroom in MB
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
   * @param type Type of the given class of environments
   * @param clusterNodeLabels All the node labels in the class of environments
   * @param primaryUsedVcores The number of virtual cores primary tenants are
   *                          using currently
   * @param primaryUsedVcoresCeiled The ceiling of the virtual core usage by
   *                                primary tenants
   * @param secondaryUsedVcores The number of virtual cores used by secondary
   *                            tenants currently
   * @param availableVcores The number of available virtual cores
   * @param primaryUsedMemoryInMB Memory capacity in MB used by primary tenants
   *                              currently
   * @param secondaryUsedMemoryInMB Memory capacity in MB used by secondary
   *                                tenants currently
   * @param availableMemoryInMB Available memory capacity in MB
   * TODO: finalize these 2 with unicorn
   * @param maxUtilization The maximum utilization of the primary and secondary
   *                       tenants in percentage [0.0, 1.0]
   * @param avgUtilization The average utilization of the primary and secondary
   *                       tenants in percentage [0.0, 1.0]
   */
  public UtilizationRecord(UtilizationRecordType type,
                           HashSet<String> clusterNodeLabels,
                           double primaryUsedVcores,
                           int primaryUsedVcoresCeiled,
                           int secondaryUsedVcores,
                           int availableVcores,
                           int primaryUsedMemoryInMB,
                           int secondaryUsedMemoryInMB,
                           int availableMemoryInMB,
                           double maxUtilization,
                           double avgUtilization) {
    this.type = type;
    this.clusterNodeLabels = clusterNodeLabels;

    // Total capacity for virtual cores
    int vcoresCapacity = 
        primaryUsedVcoresCeiled + secondaryUsedVcores + availableVcores;
    // Headroom(short) = capacity - Current
    this.shortVcoresHeadroom = vcoresCapacity * (1.0 -
        (primaryUsedVcores + secondaryUsedVcores) / (double) vcoresCapacity);
    // Headroom(medium) = capacity - MAX(AVG, Current)
    this.mediumVcoresHeadroom = vcoresCapacity * (1.0 - 
        Math.max(avgUtilization,
                 (primaryUsedVcores + secondaryUsedVcores) /
                 (double) vcoresCapacity));
    // Headroom(long) = capacity - MAX(PEAK, Current)
    this.longVcoresHeadroom = vcoresCapacity * (1.0 -
        Math.max(maxUtilization,
                 (primaryUsedVcores + secondaryUsedVcores) /
                 (double) vcoresCapacity));
    // Headroom = current available memory
    this.memoryHeadroom = availableMemoryInMB;
  }

  /**
   * Constructor that takes the raw data and computes the headrooms, which also
   * requires the full trace of the utilization.
   *
   * @param type Type of the given class of environments
   * @param clusterNodeLabels All the node labels in the class of environments
   * @param primaryUsedVcores The number of virtual cores primary tenants are
   *                          using currently
   * @param primaryUsedVcoresCeiled The ceiling of the virtual core usage by
   *                                primary tenants
   * @param secondaryUsedVcores The number of virtual cores used by secondary
   *                            tenants currently
   * @param availableVcores The number of available virtual cores
   * @param primaryUsedMemoryInMB Memory capacity in MB used by primary tenants
   *                              currently
   * @param secondaryUsedMemoryInMB Memory capacity in MB used by secondary
   *                                tenants currently
   * @param availableMemoryInMB Available memory capacity in MB
   * TODO: finalize these 2 with unicorn
   * @param maxUtilization The maximum utilization of the primary and secondary
   *                       tenants in percentage [0.0, 1.0]
   * @param avgUtilization The average utilization of the primary and secondary
   *                       tenants in percentage [0.0, 1.0]
   * @param utilizationTrace The full utilization trace of the given record
   */
  public UtilizationRecord(UtilizationRecordType type,
                           HashSet<String> clusterNodeLabels,
                           double primaryUsedVcores,
                           int primaryUsedVcoresCeiled,
                           int secondaryUsedVcores,
                           int availableVcores,
                           int primaryUsedMemoryInMB,
                           int secondaryUsedMemoryInMB,
                           int availableMemoryInMB,
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
   * Get the type of the given class of environemnts in
   * {periodic, constant, unpredictable}.
   *
   * @return The type of the utilization record
   */
  public UtilizationRecordType getType() {
    return this.type;
  }

  /**
   * Get all the node labels in the given class of environments
   *
   * @return The node labels
   */
  public HashSet<String> getClusterNodeLabels() {
    return this.clusterNodeLabels;
  }
  
  /**
   * Get the virtual cores headroom in terms of number of virtual cores.
   *
   * @param type The type of the job regarding its runtime duration
   * @return The amount virtual core headroom
   */
  public double getVcoresHeadroom(JobType type) {
    switch(type) {
      case T_JOB_SHORT:
        return this.shortVcoresHeadroom;
      case T_JOB_MEDIUM:
        return this.mediumVcoresHeadroom;
      case T_JOB_LONG:
        return this.longVcoresHeadroom;
      default:
        LOG.error("Math is broken.");
        return 0.0;
    }
  }

  /**
   * Get the memory capacity headroom in MB.
   *
   * @return The amount of memory headroom in MB
   */
  public long getMemoryHeadroom() {
    return this.memoryHeadroom;
  }

  /**
   * Get the full utilization trace.
   *
   * @return The full utilization trace of the given record
   */
  public TreeMap<Integer, Double> getUtilizationTrace() {
    return this.utilizationTrace;
  }

  /**
   * Check whether a given utilization class has enough headroom (both virtual
   * cores and memory capacity) to execute a job.
   *
   * @param requiredVcores The amount of virtual cores required by the job
   * @param requiredMemory The amount of memory capacity required by the job
   * @param type The type of the task in terms of its runtime duration
   * @return True if the job can fit in, otherwise false
   */
  public boolean fits(long requiredVcores, long requiredMemory, JobType type) {
    double vcoresHeadroom = getVcoresHeadroom(type);
    return ((requiredVcores <= vcoresHeadroom) &&
            (requiredMemory <= this.memoryHeadroom));
  }

  /**
   * Calculate the maximum number of tasks that can fit in the remaining
   * headroom (both virtual cores and memory capacity).
   *
   * @param vcoresPerTask Number of virtual cores required by each single task
   * @param memoryPerTask Memory capacity required by each single task
   * @param type The type of the task in terms of its duration
   * @return The maximum number of tasks that can fit in
   */
  public int floor(int vcoresPerTask, int memoryPerTask, JobType type) {
    double vcoresHeadroom = getVcoresHeadroom(type);
    int floorByVcores = (int) Math.floor(vcoresHeadroom / vcoresPerTask);
    int floorByMemory = (int) getMemoryHeadroom() / memoryPerTask;

    return Math.min(floorByVcores, floorByMemory);
  }
}

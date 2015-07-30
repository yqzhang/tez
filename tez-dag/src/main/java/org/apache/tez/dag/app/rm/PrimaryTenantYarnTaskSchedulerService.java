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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;

import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.rm.UtilizationRecord.JobType;
import org.apache.tez.dag.app.rm.UtilizationTable;
import org.apache.tez.dag.app.rm.UtilizationTable.Tuple;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;
import org.apache.tez.dag.api.TezConfiguration;

import com.google.common.base.Joiner;

public class PrimaryTenantYarnTaskSchedulerService extends
                 YarnTaskSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(
      PrimaryTenantYarnTaskSchedulerService.class);

  private UtilizationTable utilizationTable = null;

  // A generic random generator for probabilistic scheduling
  private Random randomGenerator;

  // Whether we have already made scheduling decision for this DAG
  private String[] scheduleNodeLabelExpressions = null;
  private double[] scheduleCDF;

  // The smallest resource scheduling unit
  private int vcoresPerTask;
  private int memoryPerTask;

  // Whether we want to enable scheduling decisions that will
  // probabilistically pick from non-preferrable types of classes
  private boolean probabilisticTypeSelection;
  // Weight for the least preferrable class
  private double lowPreferenceWeight;
  // Weight for the medium preferrable class
  private double mediumPreferenceWeight;
  // Weight for the most preferrable class
  private double highPreferenceWeight;

  // threshold of critical path length between short and medium jobs
  private int thresholdShortAndMedium;
  // threshold of critical path length between medium and long jobs
  private int thresholdMediumAndLong;

  // Whether we want to enable scheduling decisions that prefer classes that
  // have smallest residual capacity, which could reduce resource fragmentation
  private boolean bestFitScheduling;

  public PrimaryTenantYarnTaskSchedulerService(
                        TaskSchedulerAppCallback appClient,
                        ContainerSignatureMatcher containerSignatureMatcher,
                        String appHostName,
                        int appHostPort,
                        String appTrackingUrl,
                        AppContext appContext) {
    super(appClient, containerSignatureMatcher, appHostName, appHostPort,
          appTrackingUrl, appContext);
    this.randomGenerator = new Random(System.currentTimeMillis());
  }

  @Override
  public synchronized void serviceInit(Configuration conf) {
    this.vcoresPerTask = conf.getInt(
        TezConfiguration.TEZ_TASK_RESOURCE_CPU_VCORES,
        TezConfiguration.TEZ_TASK_RESOURCE_CPU_VCORES_DEFAULT);

    this.memoryPerTask = conf.getInt(
        TezConfiguration.TEZ_TASK_RESOURCE_CPU_VCORES,
        TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB_DEFAULT);

    this.probabilisticTypeSelection = conf.getBoolean(
        TezConfiguration.TEZ_PROBABILISTIC_TYPE_SELECTION,
        TezConfiguration.TEZ_PROBABILISTIC_TYPE_SELECTION_DEFAULT);

    this.lowPreferenceWeight = conf.getDouble(
        TezConfiguration.TEZ_PROBABILISTIC_LOW_PREFERENCE_WEIGHT,
        TezConfiguration.TEZ_PROBABILISTIC_LOW_PREFERENCE_WEIGHT_DEFAULT);

    this.mediumPreferenceWeight = conf.getDouble(
        TezConfiguration.TEZ_PROBABILISTIC_MEDIUM_PREFERENCE_WEIGHT,
        TezConfiguration.TEZ_PROBABILISTIC_MEDIUM_PREFERENCE_WEIGHT_DEFAULT);

    this.highPreferenceWeight = conf.getDouble(
        TezConfiguration.TEZ_PROBABILISTIC_HIGH_PREFERENCE_WEIGHT,
        TezConfiguration.TEZ_PROBABILISTIC_HIGH_PREFERENCE_WEIGHT_DEFAULT);

    this.bestFitScheduling = conf.getBoolean(
        TezConfiguration.TEZ_BEST_FIT_SCHEDULING,
        TezConfiguration.TEZ_BEST_FIT_SCHEDULING_DEFAULT);

    this.thresholdShortAndMedium = conf.getInt(
        TezConfiguration.TEZ_THRESHOLD_SHORT_AND_MEDIUM,
        TezConfiguration.TEZ_THRESHOLD_SHORT_AND_MEDIUM_DEFAULT);

    this.thresholdMediumAndLong = conf.getInt(
        TezConfiguration.TEZ_THRESHOLD_MEDIUM_AND_LONG,
        TezConfiguration.TEZ_THRESHOLD_MEDIUM_AND_LONG_DEFAULT);

    // Build the utilization table
    utilizationTable = new UtilizationTable(probabilisticTypeSelection,
                                            lowPreferenceWeight,
                                            mediumPreferenceWeight,
                                            highPreferenceWeight,
                                            bestFitScheduling,
                                            conf);

    super.serviceInit(conf);
  }

  @Override
  public void allocateTask(Object task, Resource capability,
                           String[] hosts, String[] racks,
                           Priority priority, Object containerSignature,
                           Object clientCookie) {

    synchronized (scheduleNodeLabelExpressions) {
      if (scheduleNodeLabelExpressions == null) {
        // Estimate the number of tasks for the given job
        TaskAttempt attemp = (TaskAttempt) task;

        // Get the corresponding information from DAG profiling
        int longestCriticalPath =
            this.appContext.getCurrentDAG().getProfiler().getLongestCriticalPath();
        int parallelism =
            this.appContext.getCurrentDAG().getProfiler().getNumOfEntryTasks();

        // Determine the type of the task
        JobType type;
        if (longestCriticalPath < this.thresholdShortAndMedium) {
          type = JobType.T_JOB_SHORT;
        } else if (longestCriticalPath < this.thresholdMediumAndLong) {
          type = JobType.T_JOB_MEDIUM;
        } else {
          type = JobType.T_JOB_MEDIUM;
        }

        // Make the scheduling decision
        ArrayList<Tuple<Double, HashSet<String>>> scheduleList =
            utilizationTable.pickClassesByProbability(parallelism,
                                                      vcoresPerTask,
                                                      memoryPerTask,
                                                      type);

        // Build the CDF for future scheduling
        scheduleNodeLabelExpressions = new String[scheduleList.size()];
        for (int i = 0; i < scheduleList.size(); i++) {
          scheduleCDF[i] = scheduleList.get(i).getFirst();
          // TODO: Node labels
          scheduleNodeLabelExpressions[i] = 
            "(" + Joiner.on("|").join(scheduleList.get(i).getSecond()) + ")";
        }
      }
    }

    // Pick a class of environments based on probability
    double rand = this.randomGenerator.nextDouble();
    int nodeLabelExpressionIndex = UtilizationTable.lowerBound(scheduleCDF, rand);
    String nodeLabelExpression =
        scheduleNodeLabelExpressions[nodeLabelExpressionIndex];

    CRCookie cookie = new CRCookie(task, clientCookie, containerSignature);
    CookieContainerRequest request = new CookieContainerRequest(
        capability, hosts, racks, priority, nodeLabelExpression, cookie);

    super.addRequestAndTrigger(task, request, hosts, racks);
  }
}

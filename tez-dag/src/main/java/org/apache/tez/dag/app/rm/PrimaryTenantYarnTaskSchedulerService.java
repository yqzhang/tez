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

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import org.json.JSONObject;

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

import org.apache.commons.io.IOUtils;

import com.google.common.base.Joiner;

public class PrimaryTenantYarnTaskSchedulerService extends
                 YarnTaskSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(
      PrimaryTenantYarnTaskSchedulerService.class);

  private UtilizationTable utilizationTable = null;

  // A generic random generator for probabilistic scheduling
  private Random randomGenerator;

  // Whether we have already made scheduling decision for this DAG
  private Boolean hasMadeSchedule = false;
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
  private double thresholdShortAndMedium;
  // threshold of critical path length between medium and long jobs
  private double thresholdMediumAndLong;

  // Whether we want to enable scheduling decisions that prefer classes that
  // have smallest residual capacity, which could reduce resource fragmentation
  private boolean bestFitScheduling;

  // Whether to enable scheduling decisions based on historical executions
  private boolean dagExecutionHistoryEnabled;
  private String dagExecutionHistoryPath;
  private JSONObject dagExecutionHistory;

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

    this.thresholdShortAndMedium = conf.getDouble(
        TezConfiguration.TEZ_THRESHOLD_SHORT_AND_MEDIUM,
        TezConfiguration.TEZ_THRESHOLD_SHORT_AND_MEDIUM_DEFAULT);

    this.thresholdMediumAndLong = conf.getDouble(
        TezConfiguration.TEZ_THRESHOLD_MEDIUM_AND_LONG,
        TezConfiguration.TEZ_THRESHOLD_MEDIUM_AND_LONG_DEFAULT);

    this.dagExecutionHistoryEnabled = conf.getBoolean(
        TezConfiguration.TEZ_DAG_EXECUTION_HISTORY_ENABLED,
        TezConfiguration.TEZ_DAG_EXECUTION_HISTORY_ENABLED_DEFAULT);

    this.dagExecutionHistoryPath = conf.get(
        TezConfiguration.TEZ_DAG_EXECUTION_HISTORY_PATH,
        TezConfiguration.TEZ_DAG_EXECUTION_HISTORY_PATH_DEFAULT);

    // Build the utilization table
    utilizationTable = new UtilizationTable(probabilisticTypeSelection,
                                            lowPreferenceWeight,
                                            mediumPreferenceWeight,
                                            highPreferenceWeight,
                                            bestFitScheduling,
                                            conf);
    // Build the historical execution database
    if (this.dagExecutionHistoryEnabled) {
      File historyProfile = new File(this.dagExecutionHistoryPath);
      if (historyProfile.exists() && !historyProfile.isDirectory()) {
        try {
          InputStream is = new FileInputStream(historyProfile);
          String jsonText = IOUtils.toString(is);
          this.dagExecutionHistory = new JSONObject(jsonText);
        } catch (FileNotFoundException e) {
          LOG.warn("Can not find the specified history profile: " +
                   historyProfile);
          this.dagExecutionHistoryEnabled = false;
        } catch (IOException e) {
          LOG.warn("Error reading the specified history profile: " +
                   historyProfile);
          this.dagExecutionHistoryEnabled = false;
        }
      } else {
        LOG.warn("Specified DAG execution history profile does not exist: " +
                 historyProfile);
        this.dagExecutionHistoryEnabled = false;
      }
    }

    super.serviceInit(conf);
  }

  @Override
  public void allocateTask(Object task, Resource capability,
                           String[] hosts, String[] racks,
                           Priority priority, Object containerSignature,
                           Object clientCookie) {

    synchronized (this.hasMadeSchedule) {
      if (!this.hasMadeSchedule) {
        // Estimate the number of tasks for the given job
        TaskAttempt attemp = (TaskAttempt) task;

        // Get the corresponding information from DAG profiling
        int maximumConcurrentTasks =
            this.appContext.getCurrentDAG().getProfiler().getMaximumConcurrentTasks();
        String jobHash =
            this.appContext.getCurrentDAG().getProfiler().getJobHash();

        // Determine the type of the task
        JobType type = JobType.T_JOB_MEDIUM;
        if (this.dagExecutionHistoryEnabled) {
          LOG.info("Incoming job hash: " + jobHash);
          if (this.dagExecutionHistory.has(jobHash)) {
            double totalDuration = this.dagExecutionHistory.getDouble(jobHash);

            if (totalDuration < this.thresholdShortAndMedium) {
              type = JobType.T_JOB_SHORT;

              LOG.info("Job Maximum Concurrent Tasks: " + maximumConcurrentTasks +
                       ", DAG History Execution: " + totalDuration +
                       ", Job Type: SHORT.");
            } else if (totalDuration < this.thresholdMediumAndLong) {
              type = JobType.T_JOB_MEDIUM;

              LOG.info("Job Maximum Concurrent Tasks: " + maximumConcurrentTasks +
                       ", DAG History Execution: " + totalDuration+
                       ", Job Type: MEDIUM.");
            } else {
              type = JobType.T_JOB_LONG;

              LOG.info("Job Maximum Concurrent Tasks: " + maximumConcurrentTasks +
                       ", DAG History Execution: " + totalDuration +
                       ", Job Type: LONG.");
            }
          } else {
            LOG.warn("Wait, how come we have not seen this job before?!");
          }
        }

        // Make the scheduling decision
        utilizationTable.updateUtilization();
        ArrayList<Tuple<Double, HashSet<String>>> scheduleList =
            utilizationTable.pickClassesByProbability(maximumConcurrentTasks,
                                                      vcoresPerTask,
                                                      memoryPerTask,
                                                      type);

        // Build the CDF for future scheduling
        scheduleCDF = new double[scheduleList.size()];
        scheduleNodeLabelExpressions = new String[scheduleList.size()];
        for (int i = 0; i < scheduleList.size(); i++) {
          scheduleCDF[i] = scheduleList.get(i).getFirst();
          // Put node label expression as ANY
          if (scheduleList.get(i).getSecond().size() == 0) {
            scheduleNodeLabelExpressions[i] = "*";
          // Join the labels with "|"
          } else {
            scheduleNodeLabelExpressions[i] = 
              Joiner.on("|").join(scheduleList.get(i).getSecond());
          }
        }

        LOG.info("Scheduling decision has been made:");
        for (int i = 0; i < this.scheduleCDF.length; i++) {
          LOG.info(" * CDF: " + this.scheduleCDF[i] +
                   ", node labe expression: " +
                   this.scheduleNodeLabelExpressions[i]);
        }

        this.hasMadeSchedule = true;
      }
    }

    // Pick a class of environments based on probability
    double rand = this.randomGenerator.nextDouble();
    int nodeLabelExpressionIndex = UtilizationTable.lowerBound(scheduleCDF,
                                                               rand);
    String nodeLabelExpression =
        scheduleNodeLabelExpressions[nodeLabelExpressionIndex];

    LOG.info("Node label expression: " + nodeLabelExpression);

    CRCookie cookie = new CRCookie(task, clientCookie, containerSignature);
    CookieContainerRequest request = new CookieContainerRequest(
        capability, hosts, racks, priority, nodeLabelExpression, cookie);

    super.addRequestAndTrigger(task, request, hosts, racks);
  }
}

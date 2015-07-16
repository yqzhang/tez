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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;

import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.rm.UtilizationTable;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;
import org.apache.tez.dag.api.TezConfiguration;

import com.google.common.base.Joiner;

public class PrimaryTenantYarnTaskSchedulerService extends
                 YarnTaskSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(
      PrimaryTenantYarnTaskSchedulerService.class);

  private final UtilizationTable utilizationTable = new UtilizationTable();

  // Whether we have already made scheduling decision for this DAG
  private final String scheduledNodeLabelExpression;
  private final int vcoresPerTask;
  private final int memoryPerTask;

  @Override
  public synchronized void serviceInit(Configuration conf) {
    cpuVcorePerTask = conf.getInt(
        TezConfiguration.TEZ_TASK_RESOURCE_CPU_VCORES,
        TezConfiguration.TEZ_TASK_RESOURCE_CPU_VCORES_DEFAULT);

    memoryMbPerTask = conf.getInt(
        TezConfiguration.TEZ_TASK_RESOURCE_CPU_VCORES,
        TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB_DEFAULT);

    super.serviceInit(conf);
  }

  @Override
  public void allocateTask(Object task, Resource capability,
                           String[] hosts, String[] racks,
                           Priority priority, Object containerSignature,
                           Object clientCookie) {
    synchronized (scheduledNodeLabelExpression) {
      if (scheduledNodeLabelExpression == null) {
        // 1. filter out the classes that do not have enough headroom
        TaskAttempt attemp = (TaskAttempt) task;
        int parallelism = attemp.getVertex().getTotalTasks();
        // in case of overflow
        long totalVcores = vcoresPerTask * parallalism;
        long totalMemory = memoryPerTask * parallalism;

        // 2. sort classes by headroom
        // 3. randomly pick one based on headroom
      }
    }

    CRCookie cookie = new CRCookie(task, clientCookie, containerSignature);
    CookieContainerRequest request = new CookieContainerRequest(
        capability, hosts, racks, priority, scheduledNodeLabelExpression,
        cookie);

    super.addRequestAndTrigger(task, request, hosts, racks);
  }
}

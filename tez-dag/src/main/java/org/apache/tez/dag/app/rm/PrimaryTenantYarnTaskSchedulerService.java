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
import java.util.List;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.rm.UtilizationTable;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;
import org.apache.tez.dag.records.TezVertexID;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class PrimaryTenantYarnTaskSchedulerService extends
                 YarnTaskSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(
      PrimaryTenantYarnTaskSchedulerService.class);

  final YarnClient yarnClient;

  // Maintain a map between vertex and <node labels, tasks>
  private ConcurrentHashMap<TezVertexID, VertexAssignment> vertexMap =
      new ConcurrentHashMap<TezVertexID, VertexAssignment>();

  // Maintain a map between node labels and resources assigned to tasks
  private ConcurrentHashMap<HashSet<String>, Resource> assignedResource =
      new ConcurrentHashMap<HashSet<String>, Resource>();

  private ConcurrentHashMap<Object, TezVertexID> taskToVertex =
      new ConcurrentHashMap<Object, TezVertexID>();

  private UtilizationTable utilizationTable = new UtilizationTable();

  public PrimaryTenantYarnTaskSchedulerService(
                        TaskSchedulerAppCallback appClient,
                        ContainerSignatureMatcher containerSignatureMatcher,
                        String appHostName,
                        int appHostPort,
                        String appTrackingUrl,
                        AppContext appContext) {
    super(appClient, containerSignatureMatcher, appHostName, appHostPort,
          appTrackingUrl, appContext);
    this.yarnClient = YarnClient.createYarnClient();
    // TODO: initialize the utilization table
    this.utilizationTable.updateUtilization();
  }

  @Override
  public synchronized void serviceInit(Configuration conf) {
    yarnClient.init(conf);

    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() {
    yarnClient.start();
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws InterruptedException {
    yarnClient.stop();
    super.serviceStop();
  }

  @Override
  public void allocateTask(Object task, Resource capability,
                           String[] hosts, String[] racks,
                           Priority priority, Object containerSignature,
                           Object clientCookie) {
    // Figure out what vertex this task is coming from
    TaskAttempt attempt = (TaskAttempt) task;
    TezVertexID vertexId = attempt.getVertexID();
    int parallelism = attempt.getVertex().getTotalTasks();

    // Create an entry in the map if this is the first task from the vertex
    boolean vertexAlreadyExist = true;
    synchronized (vertexMap) {
      vertexAlreadyExist = vertexMap.containsKey(vertexId) ? true : false;
    }
    
    String nodeLabelExpression;
    if (vertexAlreadyExist) {
      HashSet<String> nodeLabels = vertexMap.get(vertexId).getNodeLabels();
      nodeLabelExpression = "(" + Joiner.on("|").join(nodeLabels) + ")";
    } else {
      // Update the utilization data
      utilizationTable.updateUtilization();
      // TODO: There is where all the intelligence comes from.

      // TODO: record the resource usage
      // assignedResource
      nodeLabelExpression = "";
    }
    
    // Keep track of which vertex this task blongs to
    taskToVertex.put(task, vertexId);

    CRCookie cookie = new CRCookie(task, clientCookie, containerSignature);
    // Always relex locality
    CookieContainerRequest request = new CookieContainerRequest(
        capability, hosts, racks, priority, nodeLabelExpression, cookie);

    super.addRequestAndTrigger(task, request, hosts, racks);
  }

  @Override
  public boolean deallocateTask(Object task, boolean taskSucceeded) {
    if (taskToVertex.containsKey(task)) {
      // remove the task from the <task, vertex> mapping
      TezVertexID vertexId = taskToVertex.remove(task);
      // remove the task from the <vertex, <node labels, tasks> >
      VertexAssignment vertexAssignment = vertexMap.get(vertexId);
      vertexAssignment.removeTask(task);
      // TODO: release the resource
      // Resource assigned = assignedResource.get(vertexId);
      // assigned.free();
      // assignedResource.put();
      // remove the vertex if there isn't any tasks left
      if (vertexAssignment.getNumTasks() == 0) {
        vertexMap.remove(vertexId);
      } else {
        vertexMap.put(vertexId, vertexAssignment);
      }
    } else {
      LOG.warn("Deallocating task: " + task + " before assignment.");
    }

    return super.deallocateTask(task, taskSucceeded);
  }

  @Override
  public void assignContainer(Object task,
      Container container,
      CookieContainerRequest assigned) {
    if (taskToVertex.containsKey(task)) {
      // Update the task
      TezVertexID vertexId = taskToVertex.get(task);

      VertexAssignment vertexAssignment = vertexMap.get(vertexId);
      vertexAssignment.addTask(task);
      vertexMap.put(vertexId, vertexAssignment);
    } else {
      LOG.warn("Task " + task + " does not exsit in the taskToVertex map.");
    }

    super.assignContainer(task, container, assigned);
  }

  // TODO: assignment policy

  private class VertexAssignment {
    HashSet<String> nodeLabels = new HashSet<String>();
    List<Object> tasks = new LinkedList<Object>();

    protected VertexAssignment() { }

    protected void addNodeLabels(HashSet<String> nodeLabels) {
      this.nodeLabels.addAll(nodeLabels);
    }

    protected void setNodeLabels(HashSet<String> nodeLabels) {
      this.nodeLabels = nodeLabels;
    }

    protected HashSet<String> getNodeLabels() {
      return nodeLabels;
    }

    protected void addTask(Object task) {
      this.tasks.add(task);
    }

    protected void removeTask(Object task) {
      this.tasks.remove(task);
    }

    protected int getNumTasks() {
      return this.tasks.size();
    }
  }
}

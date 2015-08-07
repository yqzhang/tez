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

package org.apache.tez.dag.profiler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.System;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.api.TezUncheckedException;

import com.google.common.base.Joiner;

public class DAGProfiler {

  private static final Logger LOG = LoggerFactory.getLogger(
      DAGProfiler.class);

  private static final int INF = -1;
  private static final int MSEC_TO_SEC = 1000;

  private AppContext appContext;
  private DAGPlan dagPlan;
  private String dagName;

  private List<VertexPlan> vertexPlans;

  private HashMap<String, Integer> vertices;
  private HashMap<String, Edge> edges;
  private int longestCriticalPath;
  private int numOfEntryTasks;

  // Start and finish time of the DAG
  private long starttime;
  private long finishtime;

  // Number of tasks running simultaneously
  private int currentNumOfTasksRunning;
  private int maximumNumOfTasksRunning;

  // All the entry vertices
  private LinkedList<String> entryVertices;
  // Children vertices indexed by parent vertex
  private HashMap<String, Set<String>> children;
  // Record the duration of each tasks
  private HashMap<String, LinkedList<Double>> taskDuration;

  // For profiling
  private boolean dagProfilingEnabled;
  private String dagProfilingDir;


  // Moar profiling
  private int numberOfAllocatedContainers;
  private int numberOfFailedTaskAttempts;
  private int numberOfKilledTaskAttempts;

  public DAGProfiler(AppContext appContext, DAGPlan dagPlan, String dagName,
                     Configuration conf) {
    this.appContext = appContext;
    this.dagPlan = dagPlan;
    this.dagName = dagName;

    this.vertexPlans = dagPlan.getVertexList();
    this.vertices = new HashMap<String, Integer>();
    this.edges = new HashMap<String, Edge>();
    this.entryVertices = new LinkedList<String>();
    this.children = new HashMap<String, Set<String>>();
    this.taskDuration = new HashMap<String, LinkedList<Double>>();
    this.longestCriticalPath = INF;
    this.numOfEntryTasks = INF;
    this.currentNumOfTasksRunning = 0;
    this.maximumNumOfTasksRunning = 0;
    this.numberOfAllocatedContainers = 0;
    this.numberOfFailedTaskAttempts = 0;
    this.numberOfKilledTaskAttempts = 0;

    this.dagProfilingEnabled = conf.getBoolean(
        TezConfiguration.TEZ_DAG_PROFILING_ENABLED,
        TezConfiguration.TEZ_DAG_PROFILING_ENABLED_DEFAULT);
    this.dagProfilingDir = conf.get(
        TezConfiguration.TEZ_DAG_PROFILING_DIR,
        TezConfiguration.TEZ_DAG_PROFILING_DIR_DEFAULT);

    constructDAG();
  }

  protected class Edge {

    String fromVertex;
    String toVertex;

    protected Edge() { }

    protected String getFromVertex() {
      return this.fromVertex;
    }

    protected void setFromVertex(String vertex) {
      this.fromVertex = vertex;
    }
    
    protected String getToVertex() {
      return this.toVertex;
    }

    protected void setToVertex(String vertex) {
      this.toVertex = vertex;
    }
    
  }

  public void constructDAG() {

    // construct the DAG
    for (VertexPlan vertexPlan : this.vertexPlans) {
      String vertexName = vertexPlan.getName();
      this.vertices.put(vertexName, INF);

      // outgoing edges
      for (String outEdge : vertexPlan.getOutEdgeIdList()) {
        if (!this.edges.containsKey(outEdge)) {
          this.edges.put(outEdge, new Edge());
        }
        this.edges.get(outEdge).setFromVertex(vertexName);
      }

      // ingoing edges
      for (String inEdge : vertexPlan.getInEdgeIdList()) {
        if (!this.edges.containsKey(inEdge)) {
          this.edges.put(inEdge, new Edge());
        }
        this.edges.get(inEdge).setToVertex(vertexName);
      }

      // entry vertex if there isn't any edge coming in
      if (vertexPlan.getInEdgeIdList().size() == 0) {
        this.entryVertices.add(vertexName);
      }
    }

    // format the edges
    for (Edge edge : this.edges.values()) {
      String fromVertex = edge.getFromVertex();
      String toVertex = edge.getToVertex();
      // keep children information
      if (!this.children.containsKey(fromVertex)) {
        this.children.put(fromVertex, new HashSet<String>());
      }
      this.children.get(fromVertex).add(toVertex);
    }

    // compute the critical path
    setLongestCriticalPath();

    // mark the starttime
    this.starttime = System.currentTimeMillis();
  }

  public int getLongestCriticalPath() {
    return this.longestCriticalPath;
  }

  public void setLongestCriticalPath() {
    this.longestCriticalPath = INF;
    for (String vertex : this.entryVertices) {
      int distance = computeLongestCriticalPath(vertex);
      if (distance > this.longestCriticalPath) {
        this.longestCriticalPath = distance;
      }
    }
  }

  public int getNumOfEntryTasks() {
    // compute the number of tasks in entry vertices
    if (this.numOfEntryTasks == INF) {
      setNumOfEntryTasks();
    }
    return this.numOfEntryTasks;
  }

  public void setNumOfEntryTasks() {
    if (this.appContext.getCurrentDAG() == null) {
      LOG.info("Current DAG is NULL");
      return ;
    }
    this.numOfEntryTasks = 0;
    for (String vertex : this.entryVertices) {
      this.numOfEntryTasks +=
          this.appContext.getCurrentDAG().getVertex(vertex).getTotalTasks();
    }
  }

  public int computeLongestCriticalPath(String vertex) {
    if (!children.containsKey(vertex)) {
      this.vertices.put(vertex, 1);
      return 1;
    } else if (this.vertices.get(vertex) == INF) {
      for (String child : children.get(vertex)) {
        int distance = computeLongestCriticalPath(child) + 1;
        if (distance > this.vertices.get(vertex)) {
          this.vertices.put(vertex, distance);
        }
      }
    }
    return this.vertices.get(vertex);
  }

  public void finishTask(String vertexName, long duration) {
    if (!this.taskDuration.containsKey(vertexName)) {
      this.taskDuration.put(vertexName, new LinkedList<Double>());
    }
    this.taskDuration.get(vertexName).add((double) duration / MSEC_TO_SEC);
    // mark finish time
    this.finishtime = System.currentTimeMillis();
  }

  public void startTaskAttempt() {
    this.currentNumOfTasksRunning++;
    if (this.currentNumOfTasksRunning > this.maximumNumOfTasksRunning) {
      this.maximumNumOfTasksRunning = this.currentNumOfTasksRunning;
    }
  }

  public void finishTaskAttempt() {
    this.currentNumOfTasksRunning--;
    if (this.currentNumOfTasksRunning < 0) {
      this.currentNumOfTasksRunning = 0;
    }
  }

  public String getJobHash() {
    // vertex
    String[] vertexNames = new String[this.vertices.size()];
    int i = 0;
    for (String vertexName : this.vertices.keySet()) {
      if (vertexName.startsWith("Reducer")) {
        vertexNames[i] = vertexName + ":" +
            this.appContext.getCurrentDAG().getVertex(vertexName).getTotalTasks();
      } else {
        vertexNames[i] = vertexName;
      }
      i++;
    }
    Arrays.sort(vertexNames);

    // edge
    String[] edgeNames = new String[this.edges.size()];
    i = 0;
    for (Edge edge : this.edges.values()) {
      edgeNames[i] = edge.getFromVertex() + "-" + edge.getToVertex();
      i++;
    }
    Arrays.sort(edgeNames);

    return Joiner.on(",").join(vertexNames) + "," + Joiner.on(",").join(edgeNames);
  }

  public int getMaximumConcurrentTasks() {
    int[] numberOfTasksByLevel = new int[getLongestCriticalPath()];
    for (Entry<String, Integer> entry : this.vertices.entrySet()) {
      String vertexName = entry.getKey();
      int vertexLevel = entry.getValue();
      numberOfTasksByLevel[vertexLevel - 1] +=
          this.appContext.getCurrentDAG().getVertex(vertexName).getTotalTasks();
    }
    int maxNumberOfConcurrentTasks = 0;
    for (int i = 0; i < getLongestCriticalPath(); i++) {
      if (numberOfTasksByLevel[i] > maxNumberOfConcurrentTasks) {
        maxNumberOfConcurrentTasks = numberOfTasksByLevel[i];
      }
    }
    return maxNumberOfConcurrentTasks;
  }

  public void containersAllocated(int numberOfContainers) {
    this.numberOfAllocatedContainers += numberOfContainers;
  }

  public void taskAttemptFailed() {
    this.numberOfFailedTaskAttempts++;
  }

  public void taskAttemptKilled() {
    this.numberOfKilledTaskAttempts++;
  }

  public void finish() {
    /**
     * Structure of the log.
     * dagName
     * totalDuration
     * maximumNumOfTasksRunning
     * longestCriticalPath, numOfEntryVertices, numOfEntryTasks
     * numOfVertices
     * vertexName, numOfTasks, avgTaskDuration
     * ...
     * vertexName, numOfTasks, avgTaskDuration
     * numOfEdges
     * vertexFrom, vertexTo
     * ...
     * vertexFrom, vertexTo
     */
    if (this.dagProfilingEnabled) {
      try {
        // create the folder if it does not exist
        File profileDir = new File(this.dagProfilingDir);
        if (!profileDir.exists()) {
          profileDir.mkdirs();
        }
        // create the file
        String fileName = profileDir.getAbsolutePath() + "/" +
            this.dagName.replace(":", "_") + ".profile";
        FileWriter writer = new FileWriter(fileName);

        // dagName
        writer.write(this.dagName + "\n");
        // totalDuration
        double totalDuration =
            (double) (this.finishtime - this.starttime) / MSEC_TO_SEC;
        writer.write(totalDuration + "\n");
        // numberOfContainersAllocated, numberOfKilledTaskAttempts, numberOfFailedTaskAttempts
        writer.write(this.numberOfAllocatedContainers + "," +
                     this.numberOfKilledTaskAttempts + "," +
                     this.numberOfFailedTaskAttempts + "\n");
        // maximumNumOfTasksRunning
        writer.write(this.maximumNumOfTasksRunning + "\n");
        // longestCriticalPath, numOfEntryVertices, numOfEntryTasks
        writer.write(this.longestCriticalPath + "," +
                     this.entryVertices.size() + "," +
                     getNumOfEntryTasks() + "\n");
        // numOfVertices
        writer.write(this.vertices.size() + "\n");
        // vertexName, numOfTasks, avgTaskDuration
        for (String vertexName : this.vertices.keySet()) {
          int numOfTasks = 0;
          double avgDuration = 0.0;
          for (Double duration : this.taskDuration.get(vertexName)) {
            numOfTasks += 1;
            avgDuration += duration;
          }
          avgDuration /= numOfTasks;
          writer.write(vertexName + "," +
                       numOfTasks + "," +
                       avgDuration + "\n");
        }
        // numOfEdges
        writer.write(this.edges.size() + "\n");
        for (Edge edge : this.edges.values()) {
          writer.write(edge.getFromVertex() + "," + edge.getToVertex() + "\n");
        }
        writer.close();
      } catch (IOException e) {
        LOG.error("IO Exception while writing DAG profilings", e);
        throw new TezUncheckedException(e);
      }
    }
  }

}

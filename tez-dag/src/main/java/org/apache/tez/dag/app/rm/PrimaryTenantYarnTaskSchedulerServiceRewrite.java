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
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.rm.TaskSchedulerService.TaskSchedulerAppCallback;
import org.apache.tez.dag.app.rm.TaskSchedulerService.TaskSchedulerAppCallback.AppFinalStatus;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.records.TezVertexID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * TODO: Start with an extension on top of the regular YARN task scheduler,
 * and we might need to change this to a completely different class later
 * if we want to do much fancier stuff here
 */
public class PrimaryTenantYarnTaskSchedulerServiceRewrite
                 extends TaskSchedulerService
                 implements AMRMClientAsync.CallbackHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      PrimaryTenantYarnTaskSchedulerService.class);

  // Required libraries to make scheduling decisions
  final TezAMRMClientAsync<ContainerRequest> amRmClient;
  final YarnClient yarnClient;
  final TaskSchedulerAppCallback realAppClient;
  final TaskSchedulerAppCallback appClientDelegate;
  final ContainerSignatureMatcher containerSignatureMatcher;
  ExecutorService appCallbackExecutor;

  // Information about the application
  final String appHostName;
  final int appHostPort;
  final String appTrackingUrl;
  final AppContext appContext;

  private AtomicBoolean hasUnregistered = new AtomicBoolean(false);
  AtomicBoolean isStopped = new AtomicBoolean(false);

  Resource totalResources;

  Set<NodeId> blacklistedNodes = Collections
      .newSetFromMap(new ConcurrentHashMap<NodeId, Boolean>());

  // Container Re-Use configuration
  private boolean shouldReuseContainers;
  private boolean reuseRackLocal;
  private boolean reuseNonLocal;

  // Maintain a map between vertex and node labels
  private ConcurrentHashMap<TezVertexID, String> vertexMap =
      new ConcurrentHashMap<TezVertexID, String>();

  @VisibleForTesting
  protected AtomicBoolean shouldUnregister = new AtomicBoolean(false);

  /**
   * Constructor
   *
   * @param appClient An instance of the appliaction client
   * @param containerSignatureMatcher An utility to check container signature
   * @param appHostName Host name of the application
   * @param appTrackingUrl The URL for tracking application progress
   * @param appContext Context of the appliaction
   */
  public PrimaryTenantYarnTaskSchedulerServiceRewrite(
                        TaskSchedulerAppCallback appClient,
                        ContainerSignatureMatcher containerSignatureMatcher,
                        String appHostName,
                        int appHostPort,
                        String appTrackingUrl,
                        AppContext appContext) {
    super(PrimaryTenantYarnTaskSchedulerService.class.getName());

    this.amRmClient = TezAMRMClientAsync.createAMRMClientAsync(1000, this);
    this.yarnClient = YarnClient.createYarnClient();
    this.realAppClient = appClient;
    this.appCallbackExecutor = createAppCallbackExecutorService();
    this.containerSignatureMatcher = containerSignatureMatcher;
    this.appClientDelegate = createAppCallbackDelegate(appClient);
    this.appHostName = appHostName;
    this.appHostPort = appHostPort;
    this.appTrackingUrl = appTrackingUrl;
    this.appContext = appContext;

    this.totalResources = Resources.clone(this.getAvailableResources());
  }

  @VisibleForTesting
  ExecutorService createAppCallbackExecutorService() {
    return Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("TaskSchedulerAppCaller #%d").setDaemon(true).build());
  }

  TaskSchedulerAppCallback createAppCallbackDelegate(
      TaskSchedulerAppCallback realAppClient) {
    return new TaskSchedulerAppCallbackWrapper(realAppClient,
        appCallbackExecutor);
  }

  @Override
  public synchronized void serviceInit(Configuration conf) {
    
    amRmClient.init(conf);
    yarnClient.init(conf);

    int heartbeatIntervalMax = conf.getInt(
        TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX,
        TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX_DEFAULT);
    amRmClient.setHeartbeatInterval(heartbeatIntervalMax);

    shouldReuseContainers = conf.getBoolean(
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED,
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED_DEFAULT);
    reuseRackLocal = conf.getBoolean(
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED,
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED_DEFAULT);
    reuseNonLocal = conf
      .getBoolean(
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED,
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED_DEFAULT);
    Preconditions.checkArgument(
      ((!reuseRackLocal && !reuseNonLocal) || (reuseRackLocal)),
      "Re-use Rack-Local cannot be disabled if Re-use Non-Local has been"
      + " enabled");

    /*
    localitySchedulingDelay = conf.getLong(
      TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS,
      TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS_DEFAULT);
    Preconditions.checkArgument(localitySchedulingDelay >= 0,
        "Locality Scheduling delay should be >=0");

    idleContainerTimeoutMin = conf.getLong(
        TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS,
        TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS_DEFAULT);
    Preconditions.checkArgument(idleContainerTimeoutMin >= 0 || idleContainerTimeoutMin == -1,
      "Idle container release min timeout should be either -1 or >=0");
    
    idleContainerTimeoutMax = conf.getLong(
        TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS,
        TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS_DEFAULT);
    Preconditions.checkArgument(
        idleContainerTimeoutMax >= 0 && idleContainerTimeoutMax >= idleContainerTimeoutMin,
        "Idle container release max timeout should be >=0 and >= " + 
        TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS);
    
    sessionNumMinHeldContainers = conf.getInt(TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS, 
        TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS_DEFAULT);
    Preconditions.checkArgument(sessionNumMinHeldContainers >= 0, 
        "Session minimum held containers should be >=0");
    
    preemptionPercentage = conf.getInt(TezConfiguration.TEZ_AM_PREEMPTION_PERCENTAGE, 
        TezConfiguration.TEZ_AM_PREEMPTION_PERCENTAGE_DEFAULT);
    Preconditions.checkArgument(preemptionPercentage >= 0 && preemptionPercentage <= 100,
        "Preemption percentage should be between 0-100");

    numHeartbeatsBetweenPreemptions = conf.getInt(
        TezConfiguration.TEZ_AM_PREEMPTION_HEARTBEATS_BETWEEN_PREEMPTIONS,
        TezConfiguration.TEZ_AM_PREEMPTION_HEARTBEATS_BETWEEN_PREEMPTIONS_DEFAULT);
    Preconditions.checkArgument(numHeartbeatsBetweenPreemptions >= 1, 
        "Heartbeats between preemptions should be >=1");
    */

    // TODO
    // delayedContainerManager = new DelayedContainerManager();
    /*
    LOG.info("TaskScheduler initialized with configuration: " +
            "maxRMHeartbeatInterval: " + heartbeatIntervalMax +
            ", containerReuseEnabled: " + shouldReuseContainers +
            ", reuseRackLocal: " + reuseRackLocal +
            ", reuseNonLocal: " + reuseNonLocal + 
            ", localitySchedulingDelay: " + localitySchedulingDelay +
            ", preemptionPercentage: " + preemptionPercentage +
            ", numHeartbeatsBetweenPreemptions: " + numHeartbeatsBetweenPreemptions +
            ", idleContainerMinTimeout: " + idleContainerTimeoutMin +
            ", idleContainerMaxTimeout: " + idleContainerTimeoutMax +
            ", sessionMinHeldContainers: " + sessionNumMinHeldContainers);
    */
  }

  @Override
  public void serviceStart() {
    try {
      RegisterApplicationMasterResponse response;
      synchronized (this) {
        amRmClient.start();
        yarnClient.start();
        response = amRmClient.registerApplicationMaster(appHostName,
                                                        appHostPort,
                                                        appTrackingUrl);
      }
      // upcall to app outside locks
      appClientDelegate.setApplicationRegistrationData(
          response.getMaximumResourceCapability(),
          response.getApplicationACLs(),
          response.getClientToAMTokenMasterKey());
    } catch (YarnException e) {
      LOG.error("Yarn Exception while registering ", e);
      throw new TezUncheckedException(e);
    } catch (IOException e) {
      LOG.error("IO Exception while registering ", e);
      throw new TezUncheckedException(e);
    }
  }

  @Override
  public void serviceStop() throws InterruptedException {
    // upcall to app outside of locks
    try {
      synchronized (this) {
        isStopped.set(true);
        if (shouldUnregister.get()) {
          AppFinalStatus status = appClientDelegate.getFinalAppStatus();
          LOG.info("Unregistering application from RM"
              + ", exitStatus=" + status.exitStatus
              + ", exitMessage=" + status.exitMessage
              + ", trackingURL=" + status.postCompletionTrackingUrl);
          amRmClient.unregisterApplicationMaster(status.exitStatus,
              status.exitMessage,
              status.postCompletionTrackingUrl);
          LOG.info("Successfully unregistered application from RM");
          hasUnregistered.set(true);
        }
      }

      // call client.stop() without lock client will attempt to stop the callback
      // operation and at the same time the callback operation might be trying
      // to get our lock.
      amRmClient.stop();
      yarnClient.stop();
      appCallbackExecutor.shutdown();
      appCallbackExecutor.awaitTermination(1000l, TimeUnit.MILLISECONDS);
    } catch (YarnException e) {
      LOG.error("Yarn Exception while unregistering ", e);
      throw new TezUncheckedException(e);
    } catch (IOException e) {
      LOG.error("IOException while unregistering ", e);
      throw new TezUncheckedException(e);
    }
  }

  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) { }

  @Override
  public void onContainersAllocated(List<Container> containers) { }

  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
    if (isStopped.get()) {
      return;
    }
    // ignore bad nodes for now
    // upcall to app must be outside locks
    appClientDelegate.nodesUpdated(updatedNodes);
  }

  @Override
  public void onError(Throwable t) {
    if (isStopped.get()) {
      return;
    }
    appClientDelegate.onError(t);
  }

  @Override
  public void onShutdownRequest() {
    if (isStopped.get()) {
      return;
    }
    // upcall to app must be outside locks
    appClientDelegate.appShutdownRequested();
  }

  @Override
  public float getProgress() {
    if (isStopped.get()) {
      return 1;
    }

    // TODO

    return appClientDelegate.getProgress();
  }

  @Override
  public Resource getAvailableResources() {
    return amRmClient.getAvailableResources();
  }

  @Override
  public int getClusterNodeCount() {
    return amRmClient.getClusterNodeCount();
  }

  @Override
  public void dagComplete() { }

  @Override
  public Resource getTotalResources() {
    return totalResources;
  }

  @Override
  public void blacklistNode(NodeId nodeId) {
    LOG.info("Blacklisting node: " + nodeId);
    amRmClient.addNodeToBlacklist(nodeId);
    blacklistedNodes.add(nodeId);
  }

  @Override
  public void unblacklistNode(NodeId nodeId) {
    if (blacklistedNodes.remove(nodeId)) {
      LOG.info("UnBlacklisting node: " + nodeId);
      amRmClient.removeNodeFromBlacklist(nodeId);
    }
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
      nodeLabelExpression = vertexMap.get(vertexId);
    } else {
      // TODO: There is where all the intelligence comes from.
      nodeLabelExpression = "";
    }

    // Always relex locality
    ContainerRequest request = new ContainerRequest(
        capability, hosts, racks, priority, true, nodeLabelExpression);
  }

  @Override
  public void allocateTask(Object task, Resource capability,
                           ContainerId containerId, Priority priority,
                           Object containerSignature, Object clientCookie) { }

  @Override
  public boolean deallocateTask(Object task, boolean taskSucceeded) {
    return true;
  }

  @Override
  public Object deallocateContainer(ContainerId containerId) {
    return null;
  }

  @Override
  public void setShouldUnregister() {
    this.shouldUnregister.set(true);
  }

  @Override
  public boolean hasUnregistered() {
    return hasUnregistered.get();
  }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.rm.TaskSchedulerService.TaskSchedulerAppCallback;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;

/**
 * TODO: Start with an extension on top of the regular YARN task scheduler,
 * and we might need to change this to a completely different class later
 * if we want to do much fancier stuff here
 */
public class PrimaryTenantYarnTaskSchedulerService extends 
                 YarnTaskSchedulerService {
  private static final Logger LOG = LoggerFactory.getLogger(
      PrimaryTenantYarnTaskSchedulerService.class);

  public PrimaryTenantYarnTaskSchedulerService(
                        TaskSchedulerAppCallback appClient,
                        ContainerSignatureMatcher containerSignatureMatcher,
                        String appHostName,
                        int appHostPort,
                        String appTrackingUrl,
                        AppContext appContext) {
    super(appClient, containerSignatureMatcher, appHostName, appHostPort,
          appTrackingUrl, appContext);
  }
}

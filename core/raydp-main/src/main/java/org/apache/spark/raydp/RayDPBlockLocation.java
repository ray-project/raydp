/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.raydp;

import java.io.Serializable;

/** Spark cache location and the corresponding named Ray actor id. */
public final class RayDPBlockLocation implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String executorId;
  private final String actorId;

  public RayDPBlockLocation(String executorId, String actorId) {
    this.executorId = executorId;
    this.actorId = actorId;
  }

  public String getExecutorId() {
    return executorId;
  }

  /** Returns RayDP's actor-name suffix, such as the 0 in raydp-executor-0. */
  public String getActorId() {
    return actorId;
  }
}

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

package org.apache.spark.deploy.raydp;

import java.util.List;
import java.util.Optional;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;

public class ExternalShuffleServiceUtils {
  private static String getShuffleServiceActorName(String node) {
    return "raydp-shuffle-service-" + node.replace('.', '-');
  }

  public static ActorHandle<RayExternalShuffleService> createShuffleService(
      String node, List<String> options) {
    String actorName = getShuffleServiceActorName(node);
    Optional<ActorHandle<RayExternalShuffleService>> existing = Ray.getActor(actorName);
    if (existing.isPresent()) {
      return existing.get();
    }

    return Ray.actor(RayExternalShuffleService::new)
              .setName(actorName)
              .setResource("node:" + node, 0.01)
              .setJvmOptions(options)
              .setMaxRestarts(-1)
              .setMaxTaskRetries(-1)
              .remote();
  }

  public static void stopShuffleService(
      ActorHandle<RayExternalShuffleService> handle) {
    handle.task(RayExternalShuffleService::stop).remote();
  }
}

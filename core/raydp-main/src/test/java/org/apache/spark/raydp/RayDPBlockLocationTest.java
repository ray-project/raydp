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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class RayDPBlockLocationTest {
  @Test
  public void unchangedExecutorsKeepTheirActorIds() {
    RayDPBlockLocation[] locations = RayExecutorUtils.resolveBlockLocations(
        new String[] {"7", "0", "7"}, Collections.emptyMap());

    assertEquals(3, locations.length);
    assertLocation(locations[0], "7", "7");
    assertLocation(locations[1], "0", "0");
    assertLocation(locations[2], "7", "7");
  }

  @Test
  public void restartedExecutorsUseOriginalActorIdsInPartitionOrder() {
    Map<String, String> restarted = new HashMap<>();
    restarted.put("1", "0");
    restarted.put("4", "3");
    RayDPBlockLocation[] locations = RayExecutorUtils.resolveBlockLocations(
        new String[] {"1", "4", "1"}, restarted);

    assertEquals(3, locations.length);
    assertLocation(locations[0], "1", "0");
    assertLocation(locations[1], "4", "3");
    assertLocation(locations[2], "1", "0");
  }

  @Test
  public void mixedLocationsMapOnlyRestartedExecutors() {
    RayDPBlockLocation[] locations = RayExecutorUtils.resolveBlockLocations(
        new String[] {"7", "2", "7"}, Collections.singletonMap("2", "0"));

    assertEquals(3, locations.length);
    assertLocation(locations[0], "7", "7");
    assertLocation(locations[1], "2", "0");
    assertLocation(locations[2], "7", "7");
  }

  @Test
  public void emptyLocationsStayEmpty() {
    RayDPBlockLocation[] locations = RayExecutorUtils.resolveBlockLocations(
        new String[0], Collections.emptyMap());

    assertEquals(0, locations.length);
  }

  private static void assertLocation(
      RayDPBlockLocation location, String executorId, String actorId) {
    assertEquals(executorId, location.getExecutorId());
    assertEquals(actorId, location.getActorId());
  }
}

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

package org.apache.spark.deploy.raydp

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import io.ray.api.ActorHandle

import org.apache.spark.internal.Logging

object DriverAppMasterReporter extends Logging {

  private val reported = new AtomicBoolean(false)

  private var appId: String = null
  private var masterHandle: ActorHandle[RayAppMaster] = null

  def reset(): Unit = synchronized {
    reported.set(false)
    appId = null
    masterHandle = null
  }

  def bind(appId: String): Unit = synchronized {
    if (!reported.get()) {
      this.appId = appId
    }
  }

  def bindMasterHandle(masterHandle: ActorHandle[RayAppMaster]): Unit = synchronized {
    if (!reported.get()) {
      this.masterHandle = masterHandle
    }
  }

  def tryReportAndCleanup(): Boolean = {
    val snapshot = DriverExitState.current()
    if (!DriverExitState.isTerminal(snapshot.state)) {
      logDebug(s"Skip AppMaster report because driver state is not terminal: ${snapshot.state}")
      false
    } else {
      val binding = synchronized {
        if (!reported.compareAndSet(false, true)) None
        else Some((appId, masterHandle))
      }
      binding match {
        case None => false
        case Some((currentAppId, currentMasterHandle)) =>
          try {
            if (currentAppId != null && currentMasterHandle != null) {
              RayAppMasterUtils.finishApplication(
                currentMasterHandle,
                currentAppId,
                snapshot.state.toString,
                snapshot.exitCode,
                snapshot.diagnostics)
            } else {
              logWarning("Skip reporting terminal application state because AppMaster binding " +
                "is incomplete.")
            }
          } catch {
            case NonFatal(e) =>
              logWarning("Failed to report terminal application state to AppMaster", e)
          } finally {
            if (currentMasterHandle != null) {
              try {
                RayAppMasterUtils.stopAppMaster(currentMasterHandle)
              } catch {
                case NonFatal(e) =>
                  logWarning("Failed to stop AppMaster during driver cleanup", e)
              }
            }
            synchronized {
              appId = null
              masterHandle = null
            }
          }
          true
      }
    }
  }
}

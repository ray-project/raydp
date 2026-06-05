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

object DriverExitState {

  case class Snapshot(state: ApplicationState.Value, exitCode: Int, diagnostics: String)

  private var snapshot = Snapshot(ApplicationState.UNKNOWN, 0, null)

  def reset(): Unit = synchronized {
    snapshot = Snapshot(ApplicationState.UNKNOWN, 0, null)
  }

  def current(): Snapshot = synchronized {
    snapshot
  }

  def isTerminal(state: ApplicationState.Value): Boolean = {
    state == ApplicationState.FINISHED ||
      state == ApplicationState.FAILED ||
      state == ApplicationState.KILLED
  }

  def trySetFinished(): Boolean = synchronized {
    trySet(ApplicationState.FINISHED, 0, null)
  }

  def trySetFailed(exitCode: Int, diagnostics: String): Boolean = synchronized {
    trySet(ApplicationState.FAILED, normalizedFailureCode(exitCode), diagnostics)
  }

  def trySetKilled(exitCode: Int, diagnostics: String): Boolean = synchronized {
    val normalizedExitCode = if (exitCode == 0) {
      143
    } else {
      exitCode
    }
    trySet(ApplicationState.KILLED, normalizedExitCode, diagnostics)
  }

  private def trySet(
      state: ApplicationState.Value,
      exitCode: Int,
      diagnostics: String): Boolean = {
    if (isTerminal(snapshot.state)) {
      false
    } else {
      snapshot = Snapshot(state, exitCode, diagnostics)
      true
    }
  }

  private def normalizedFailureCode(exitCode: Int): Int = {
    if (exitCode == 0) {
      1
    } else {
      exitCode
    }
  }
}

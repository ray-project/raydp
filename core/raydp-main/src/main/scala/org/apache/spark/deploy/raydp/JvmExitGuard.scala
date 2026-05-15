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

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils
import scala.concurrent.duration._

/**
 * Force JVM process to exit after the Spark application ends, even when non-daemon
 * threads (e.g., Hudi Embedded Timeline Server) prevent natural JVM termination.
 *
 * When running Spark on Ray via `raydp-submit`, the JVM process started by the shell
 * script may hang after `SparkContext.stop()` because certain non-daemon threads keep
 * the JVM alive. KubeRay RayJob relies on the entrypoint process exiting to determine
 * job completion, so this causes RayJobs to stay in RUNNING state indefinitely.
 *
 * This guard starts a daemon countdown thread when the driver reaches a terminal state.
 * If the JVM hasn't exited within the configured timeout, it calls `System.exit()` to
 * force termination.
 *
 * Configuration via JVM system properties:
 *   -Draydp.jvm.exit.timeout=300  (seconds, default: 300, 0 = disabled)
 */
object JvmExitGuard extends Logging {

  val EXIT_SUCCESS = 0
  val EXIT_APP_FAILED = 1
  val EXIT_KILLED = 143

  /** Timeout in seconds before forced exit. Read from system property, default 300. */
  val EXIT_TIMEOUT_SEC: Int = {
    val value = System.getProperty("raydp.jvm.exit.timeout", "300")
    try {
      value.toInt
    } catch {
      case _: NumberFormatException =>
        logWarning(s"Invalid raydp.jvm.exit.timeout value: $value, using default 300")
        300
    }
  }

  /** Guard state, protected by synchronized on the singleton object. */
  private var triggered = false
  private var exitCode = EXIT_SUCCESS

  /**
   * Arm the delayed exit guard after the driver reaches a terminal state.
   */
  def arm(appExitCode: Int = EXIT_SUCCESS): Unit = {
    if (EXIT_TIMEOUT_SEC <= 0) {
      logInfo("JvmExitGuard: disabled (raydp.jvm.exit.timeout <= 0)")
      return
    }

    JvmExitGuard.synchronized {
      if (triggered) {
        logDebug("JvmExitGuard: already triggered, ignoring duplicate call")
        return
      }
      triggered = true
      exitCode = appExitCode
    }

    logInfo(s"JvmExitGuard: armed delayed JVM exit (exitCode=$exitCode). " +
      s"Will force JVM exit in ${EXIT_TIMEOUT_SEC}s if it doesn't terminate naturally. " +
      s"(Configure via -Draydp.jvm.exit.timeout=N, 0=disabled)")

    // Daemon thread: does NOT prevent JVM from exiting naturally.
    // If the JVM exits on its own before the timeout, this thread is
    // simply terminated along with the JVM.
    val guardThread = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          val deadline = System.nanoTime() + EXIT_TIMEOUT_SEC.seconds.toNanos
          while (System.nanoTime() < deadline) {
            Thread.sleep(1000)
          }

          logWarning(s"JvmExitGuard: JVM still alive after ${EXIT_TIMEOUT_SEC}s. " +
            s"Forcing System.exit($exitCode).")

          try {
            System.exit(exitCode)
          } catch {
            case _: IllegalStateException =>
              // JVM is already in shutdown sequence; fall back to Runtime.halt()
              // which bypasses all shutdown hooks and terminates immediately.
              logWarning("JvmExitGuard: System.exit() failed (already shutting down), " +
                "falling back to Runtime.halt()")
              Runtime.getRuntime.halt(exitCode)
          }
        } catch {
          case _: InterruptedException =>
            logDebug("JvmExitGuard: interrupted, JVM likely exited naturally")
          case e: Throwable =>
            // If the guard thread itself is failing, force halt as last resort.
            logError(s"JvmExitGuard: unexpected error in guard thread", e)
            try {
              Runtime.getRuntime.halt(exitCode)
            } catch {
              case _: Throwable => // give up
            }
        }
      }
    }, "jvm-exit-guard")

    guardThread.setDaemon(true)
    guardThread.start()
    logDebug("JvmExitGuard: countdown thread started")
  }
}

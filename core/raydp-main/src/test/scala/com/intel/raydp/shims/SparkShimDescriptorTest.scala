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

package com.intel.raydp.shims

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Tests for Spark version parsing and minor-line shim matching.
 *
 * A shim covers a whole `major.minor` line, so a newly published patch must resolve without any
 * code change. These tests pin that, and pin the boundaries a naive prefix match would get wrong.
 */
class SparkShimDescriptorTest {

  private class TestProvider(major: Int, minor: Int)
    extends SparkMinorLineShimProvider(major, minor) {
    override def createShim: SparkShims = throw new UnsupportedOperationException("unused")
  }

  @Test
  def parsesAReleaseVersion(): Unit = {
    assertEquals(Some(SparkShimDescriptor(4, 2, 0)), SparkShimDescriptor.parse("4.2.0"))
    assertEquals(Some(SparkShimDescriptor(4, 1, 13)), SparkShimDescriptor.parse("4.1.13"))
  }

  @Test
  def ignoresAQualifierSuffix(): Unit = {
    // Spark reports versions such as 4.2.0-preview1 and 4.3.0-SNAPSHOT.
    assertEquals(Some(SparkShimDescriptor(4, 2, 0)), SparkShimDescriptor.parse("4.2.0-preview1"))
    assertEquals(Some(SparkShimDescriptor(4, 3, 0)), SparkShimDescriptor.parse("4.3.0-SNAPSHOT"))
  }

  @Test
  def rejectsAVersionWithoutAPatchComponent(): Unit = {
    assertEquals(None, SparkShimDescriptor.parse("4.2"))
    assertEquals(None, SparkShimDescriptor.parse("not-a-version"))
    assertEquals(None, SparkShimDescriptor.parse(""))
  }

  @Test
  def matchesEveryPatchOfItsOwnLine(): Unit = {
    val provider = new TestProvider(4, 1)
    // 4.1.2 and 4.1.3 are released patches that an enumerated list would have missed.
    Seq("4.1.0", "4.1.1", "4.1.2", "4.1.3", "4.1.99").foreach { version =>
      assertTrue(provider.matches(version), s"expected $version to match the 4.1 line")
    }
  }

  @Test
  def doesNotMatchAnotherMinorLine(): Unit = {
    val provider = new TestProvider(4, 1)
    Seq("4.0.4", "4.2.0", "3.5.1").foreach { version =>
      assertFalse(provider.matches(version), s"expected $version not to match the 4.1 line")
    }
  }

  @Test
  def doesNotConfuseMinorTenWithMinorOne(): Unit = {
    // A prefix match on "4.1" would wrongly claim 4.10.x.
    assertFalse(new TestProvider(4, 1).matches("4.10.0"))
    assertTrue(new TestProvider(4, 10).matches("4.10.0"))
  }

  @Test
  def doesNotMatchAnUnparseableVersion(): Unit = {
    assertFalse(new TestProvider(4, 1).matches("unknown"))
  }
}

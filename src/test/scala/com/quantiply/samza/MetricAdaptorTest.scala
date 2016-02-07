/*
 * Copyright 2014-2016 Quantiply Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quantiply.samza

import com.codahale.metrics.{JmxReporter, MetricRegistry, Gauge}
import org.apache.samza.config.MapConfig
import org.apache.samza.metrics._
import org.apache.samza.metrics.reporter.JmxReporterFactory
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class MetricAdaptorTest {
  val GROUP_NAME = "test.group"

  def createAdaptor = {
    val samzaRegistry = new MetricsRegistryMap("useless")
    val codaRegistry = new MetricRegistry()
    JmxReporter.forRegistry(codaRegistry).inDomain("samza-coda-test").build().start()

    val reporter = new JmxReporterFactory().getMetricsReporter("fakeJob", "fakeContainer", new MapConfig(Map[String, String]()))
    reporter.register("fakeSource", samzaRegistry)
    reporter.start()

    new MetricAdaptor(codaRegistry, samzaRegistry, GROUP_NAME)
  }

  def getMetric(adaptor: MetricAdaptor, name: String) = {
    adaptor.getSamzaRegistry().asInstanceOf[MetricsRegistryMap].getGroup(GROUP_NAME).get(name).asInstanceOf[MapGauge]
  }

  def getMetricValueMap(adaptor: MetricAdaptor, name: String) = {
    getMetric(adaptor, name).getValue.toMap
  }

  class MyGauge extends Gauge[Integer] {
    @Override
    def getValue = 99
  }

  @Test
  def testGauge = {
    val adaptor = createAdaptor
    val myGauge = new MyGauge
    val g = adaptor.gauge("my-gauge", myGauge)

    assertEquals(99, myGauge.getValue)

    val map = getMetricValueMap(adaptor, "my-gauge")
    assertEquals("gauge", map("type"))
    assertEquals(99, map("value"))
  }

  @Test
  def testCounter = {
    val adaptor = createAdaptor
    val c = adaptor.counter("my-counter")
    c.inc(45)

    val map = getMetricValueMap(adaptor, "my-counter")
    assertEquals("counter", map("type"))
    assertEquals(45L, map("count"))
  }

  @Test
  def testMeter = {
    val adaptor = createAdaptor
    val m = adaptor.meter("my-meter")
    m.mark(3)

    val map = getMetricValueMap(adaptor, "my-meter")
    assertEquals(3L, map("count"))
    assertEquals("meter", map("type"))
    assertEquals("SECONDS", map("rateUnit"))
    assert(Set("fifteenMinuteRate", "fiveMinuteRate", "oneMinuteRate", "meanRate").subsetOf(map.keySet))
  }

  @Test
  def testTimer = {
    val adaptor = createAdaptor
    val t = adaptor.timer("my-timer")
    t.time().stop()

    val map = getMetricValueMap(adaptor, "my-timer")
    assertEquals("timer", map("type"))
    assertEquals("NANOSECONDS", map("durationUnit"))
    assert(Set("75thPercentile", "mean", "min", "max", "99thPercentile", "95thPercentile", "median", "98thPercentile", "stdDev").subsetOf(map.keySet))
  }

  @Test
  def testHistogram = {
    val adaptor = createAdaptor
    val h = adaptor.histogram("my-hist")
    h.update(5)

    //Uncomment and attach jconsole to see what metrics look like in JMX
    //java.lang.Thread.sleep(1000000L)

    val map = getMetricValueMap(adaptor, "my-hist")
    val expected = Map(
      "type" -> "histogram",
      "samples" -> 1,
      "75thPercentile" -> 5.0,
      "mean" -> 5.0,
      "min" -> 5,
      "999thPercentile" -> 5.0,
      "max" -> 5,
      "99thPercentile" -> 5.0,
      "95thPercentile" -> 5.0,
      "median" -> 5.0,
      "98thPercentile" -> 5.0,
      "stdDev" -> 0.0
    )
    assertEquals(expected, map)
  }
}

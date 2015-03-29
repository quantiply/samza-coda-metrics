package com.quantiply.samza

import collection.JavaConversions._

import java.util

import com.codahale.metrics.MetricRegistry
import org.apache.samza.metrics._
import org.junit.Assert._
import org.junit.Test

class MetricAdaptorTest {
  val GROUP_NAME = "test.group"

  def createAdaptorAndRegistry() = {
    val samzaRegistry = new MetricsRegistryMap("useless")
    val adaptor = new MetricAdaptor(new MetricRegistry(), samzaRegistry, GROUP_NAME)
    (adaptor, samzaRegistry)
  }

  @Test
  def testCounter = {
    val (adaptor, samzaRegistry) = createAdaptorAndRegistry()

    val c = adaptor.counter("my-counter")
    c.inc(45)

    val map = samzaRegistry.getGroup(GROUP_NAME).get("my-counter").asInstanceOf[Gauge[MapGauge]].getValue.getValue.toMap
    assertEquals(Map("name" -> "my-counter", "count" -> "45"), map)
  }

  @Test
  def testMeter = {
    val (adaptor, samzaRegistry) = createAdaptorAndRegistry()

    val m = adaptor.meter("my-meter")
    m.mark(3)

    val map = samzaRegistry.getGroup(GROUP_NAME).get("my-meter").asInstanceOf[Gauge[MapGauge]].getValue.getValue.toMap
    assertEquals("3", map("count"))
    assertEquals("my-meter", map("name"))
    assert(Set("fifteenMinuteRate", "fiveMinuteRate", "oneMinuteRate", "meanRate").subsetOf(map.keySet))
  }

  @Test
  def testTimer = {
    val (adaptor, samzaRegistry) = createAdaptorAndRegistry()

    val t = adaptor.timer("my-timer")
    t.time().stop()

    val map = samzaRegistry.getGroup(GROUP_NAME).get("my-timer").asInstanceOf[Gauge[MapGauge]].getValue.getValue.toMap
    assertEquals("my-timer", map("name"))
    assert(Set("75thPercentile", "mean", "min", "max", "99thPercentile", "95thPercentile", "median", "98thPercentile", "stdDev").subsetOf(map.keySet))
  }

  @Test
  def testHistogram = {
    val (adaptor, samzaRegistry) = createAdaptorAndRegistry()

    val h = adaptor.histogram("my-hist")
    h.update(5)

    val map = samzaRegistry.getGroup(GROUP_NAME).get("my-hist").asInstanceOf[Gauge[MapGauge]].getValue.getValue.toMap
    val expected = Map(
      "75thPercentile" -> "5.0",
      "name" -> "my-hist",
      "mean" -> "5.0",
      "min" -> "5",
      "999thPercentile" -> "5.0",
      "max" -> "5",
      "99thPercentile" -> "5.0",
      "95thPercentile" -> "5.0",
      "median" -> "5.0",
      "98thPercentile" -> "5.0",
      "stdDev" -> "0.0"
    )
    assertEquals(expected, map)
  }
}

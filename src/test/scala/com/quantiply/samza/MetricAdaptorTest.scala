package com.quantiply.samza

import com.codahale.metrics.{JmxReporter, MetricRegistry}
import org.apache.samza.metrics._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class MetricAdaptorTest {
  val GROUP_NAME = "test.group"

  def createAdaptor = {
    val samzaRegistry = new MetricsRegistryMap("useless")
    val codaRegistry = new MetricRegistry()
    JmxReporter.forRegistry(codaRegistry).inDomain("samza-coda-test").build().start()
    new MetricAdaptor(codaRegistry, samzaRegistry, GROUP_NAME)
  }

  def getMetricValueMap(adaptor: MetricAdaptor, name: String) = {
    adaptor.getSamzaRegistry().asInstanceOf[MetricsRegistryMap].getGroup(GROUP_NAME).get(name).asInstanceOf[Gauge[MapGauge]].getValue.getValue.toMap
  }

  @Test
  def testCounter = {
    val adaptor = createAdaptor
    val c = adaptor.counter("my-counter")
    c.inc(45)

    val map = getMetricValueMap(adaptor, "my-counter")
    assertEquals(Map("name" -> "my-counter", "count" -> "45"), map)
  }

  @Test
  def testMeter = {
    val adaptor = createAdaptor
    val m = adaptor.meter("my-meter")
    m.mark(3)

    val map = getMetricValueMap(adaptor, "my-meter")
    assertEquals("3", map("count"))
    assertEquals("my-meter", map("name"))
    assert(Set("fifteenMinuteRate", "fiveMinuteRate", "oneMinuteRate", "meanRate").subsetOf(map.keySet))
  }

  @Test
  def testTimer = {
    val adaptor = createAdaptor
    val t = adaptor.timer("my-timer")
    t.time().stop()

    val map = getMetricValueMap(adaptor, "my-timer")
    assertEquals("my-timer", map("name"))
    assert(Set("75thPercentile", "mean", "min", "max", "99thPercentile", "95thPercentile", "median", "98thPercentile", "stdDev").subsetOf(map.keySet))
  }

  @Test
  def testHistogram = {
    val adaptor = createAdaptor
    val h = adaptor.histogram("my-hist")
    h.update(5)

//    java.lang.Thread.sleep(1000000L)

    val map = getMetricValueMap(adaptor, "my-hist")
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

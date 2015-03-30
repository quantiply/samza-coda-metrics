package com.quantiply.samza

import com.codahale.metrics.{JmxReporter, MetricRegistry}
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

  @Test
  def testCounter = {
    val adaptor = createAdaptor
    val c = adaptor.counter("my-counter")
    c.inc(45)

    val map = getMetricValueMap(adaptor, "my-counter")
    assertEquals("45", map("count"))
  }

  @Test
  def testMeter = {
    val adaptor = createAdaptor
    val m = adaptor.meter("my-meter")
    m.mark(3)

    val map = getMetricValueMap(adaptor, "my-meter")
    assertEquals("3", map("count"))
    assert(Set("fifteenMinuteRate", "fiveMinuteRate", "oneMinuteRate", "meanRate").subsetOf(map.keySet))
  }

  @Test
  def testTimer = {
    val adaptor = createAdaptor
    val t = adaptor.timer("my-timer")
    t.time().stop()

    val map = getMetricValueMap(adaptor, "my-timer")
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
      "75thPercentile" -> "5.0",
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

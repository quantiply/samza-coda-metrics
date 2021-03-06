# samza-coda-metrics
Report Coda Hale Metrics to Samza metrics topic

# Usage

Create the adapter in the init() method and use it to create/register metrics

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        MetricAdaptor adaptor = new MetricAdaptor(new MetricRegistry(), context.getMetricsRegistry(), "your.metric.group.name");
        lagFromOrigin = adaptor.histogram("lag-from-origin-ms");
    }

Use the metric in the process() method

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        ...
        long eventTimeMs = ((Long)request.get("timestamp_ms")).longValue();
        long nowMs = System.currentTimeMillis();
        lagFromOrigin.update(nowMs - eventTimeMs);
    }

See your metrics in the Samza metrics topic for the job

	{
  	  "header": {...},
      "metrics": {
        "org.apache.samza.container.TaskInstanceMetrics": {...},
        "your.metric.group.name": {
          "lag-from-origin-ms": {
            "type": "histogram",
            "samples": 28,
            "75thPercentile": 24895.0,
            "98thPercentile": 25019.0,
            "min": 672,
            "median": 722.0,
            "95thPercentile": 24995.0,
            "99thPercentile": 25024.0,
            "max": 25028,
            "mean": 8099.002017881099,
            "999thPercentile": 25028.0,
            "stdDev": 11160.633442938424
      },
      ...
 	}

#Building With Maven 3

Add the Quantiply Maven Repository

	 <repositories>
      <repository>
        <id>quantiply</id>
        <name>Quantiply Bintray Repository</name>
        <url>http://dl.bintray.com/quantiply/quantiply</url>
      </repository>
    </repositories>

Add the dependency

	 <dependencies>
      <dependency>
        <groupId>com.quantiply.samza</groupId>
        <artifactId>coda-metrics</artifactId>
        <version>1.5</version>
      </dependency>
    </dependencies>

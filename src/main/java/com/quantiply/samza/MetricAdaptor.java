package com.quantiply.samza;

import com.codahale.metrics.*;
import org.apache.samza.metrics.MetricsRegistry;

public class MetricAdaptor {
    private final MetricRegistry codaRegistry;
    private final MetricsRegistry samzaRegistry;
    private final String groupName;

    public MetricAdaptor(MetricRegistry codaRegistry, MetricsRegistry samzaRegistry, String groupName) {
        this.codaRegistry = codaRegistry;
        this.samzaRegistry = samzaRegistry;
        this.groupName = groupName;
    }

    public <T extends Metric> T register(String name, T metric) {
        codaRegistry.register(name, metric);
        return registerWithSamza(name, metric);
    }

    public Histogram histogram(String name) {
        return registerWithSamza(name, codaRegistry.histogram(name));
    }

    public Counter counter(String name) {
        return registerWithSamza(name, codaRegistry.counter(name));
    }

    public Timer timer(String name) {
        return registerWithSamza(name, codaRegistry.timer(name));
    }

    public Meter meter(String name) {
        return registerWithSamza(name, codaRegistry.meter(name));
    }

    public Gauge gauge(String name, Gauge g) {
        return register(name, g);
    }

    public String getGroupName() {
        return groupName;
    }

    public MetricRegistry getCodaRegistry() {
        return codaRegistry;
    }

    public MetricsRegistry getSamzaRegistry() {
        return samzaRegistry;
    }

    private <T extends Metric> T registerWithSamza(String name, T metric) {
        samzaRegistry.newGauge(groupName, new MapGauge(name, metric));
        return metric;
    }

}

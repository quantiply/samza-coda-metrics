package com.quantiply.samza;

import com.codahale.metrics.*;
import org.apache.samza.metrics.MetricsRegistry;

public class MetricCreator {
    private final MetricRegistry codaRegistry;
    private final MetricsRegistry samzaRegistry;
    private final String groupName;


    public MetricCreator(MetricRegistry codaRegistry, MetricsRegistry samzaRegistry, String groupName) {
        this.codaRegistry = codaRegistry;
        this.samzaRegistry = samzaRegistry;
        this.groupName = groupName;
    }

    public Histogram histogram(String name) {
        Histogram h = codaRegistry.histogram(name);
        samzaRegistry.newGauge(groupName, name, new MapGauge(name, h));
        return h;
    }

    public Counter counter(String name) {
        Counter h = codaRegistry.counter(name);
        samzaRegistry.newGauge(groupName, name, new MapGauge(name, h));
        return h;
    }

    public Timer timer(String name) {
        Timer h = codaRegistry.timer(name);
        samzaRegistry.newGauge(groupName, name, new MapGauge(name, h));
        return h;
    }

    public Meter meter(String name) {
        Meter h = codaRegistry.meter(name);
        samzaRegistry.newGauge(groupName, name, new MapGauge(name, h));
        return h;
    }

    public void gauge(String name, Gauge g) {
        samzaRegistry.newGauge(groupName, name, new MapGauge(name, g));
    }
}

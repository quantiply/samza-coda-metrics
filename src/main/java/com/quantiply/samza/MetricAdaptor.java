/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
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

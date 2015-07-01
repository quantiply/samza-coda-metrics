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
import org.apache.samza.metrics.Gauge;

import java.util.HashMap;
import java.util.Map;

public class MapGauge extends Gauge<Map<String, Object>> {

    private Metric _metric;

    public MapGauge(String name, Metric metric) {
        super(name, new HashMap<String, Object>());
        _metric = metric;
    }

    @Override
    public Map<String, Object> getValue() {
        return toMap(_metric);
    }

    private Map<String, Object> toMap(Metric metric) {
        if (metric instanceof Meter) {
            return meter((Meter) metric);
        }
        else if (metric instanceof Histogram) {
            return histogram((Histogram) metric);
        }
        else if (metric instanceof Counter) {
            return counter((Counter) metric);
        }
        else if (metric instanceof Timer) {
            return timer((Timer) metric);
        }
        else if (metric instanceof com.codahale.metrics.Gauge) {
            return gauge((com.codahale.metrics.Gauge) metric);
        }

        return null;
    }

    public Map<String, Object> meter(Meter meter) {
        Map<String,Object> data = new HashMap<>();
        data.put("type", "meter");
        data.put("count", meter.getCount());
        data.put("oneMinuteRate", meter.getOneMinuteRate());
        data.put("fiveMinuteRate", meter.getFiveMinuteRate());
        data.put("fifteenMinuteRate", meter.getFifteenMinuteRate());
        data.put("meanRate", meter.getMeanRate());
        data.put("rateUnit", "SECONDS");
        return data;
    }

    private Map<String, Object> counter(Counter counter) {
        Map<String,Object> data = new HashMap<>();
        data.put("type", "counter");
        data.put("count", counter.getCount());
        return data;

    }

    private Map<String, Object> histogram(Histogram histogram) {
        Map<String,Object> data = new HashMap<>();
        final Snapshot snapshot = histogram.getSnapshot();
        data.put("type", "histogram");
        data.put("min", snapshot.getMin());
        data.put("max", snapshot.getMax());
        data.put("mean", snapshot.getMean());
        data.put("stdDev", snapshot.getStdDev());
        data.put("median", snapshot.getMedian());
        data.put("75thPercentile", snapshot.get75thPercentile());
        data.put("95thPercentile", snapshot.get95thPercentile());
        data.put("98thPercentile", snapshot.get98thPercentile());
        data.put("99thPercentile", snapshot.get99thPercentile());
        data.put("999thPercentile", snapshot.get999thPercentile());
        return data;
    }

    private Map<String, Object> timer(Timer timer) {
        Map<String,Object> data = new HashMap<>();
        final Snapshot snapshot = timer.getSnapshot();
        data.put("type", "timer");
        data.put("min", snapshot.getMin());
        data.put("max", snapshot.getMax());
        data.put("mean", snapshot.getMean());
        data.put("stdDev", snapshot.getStdDev());
        data.put("median", snapshot.getMedian());
        data.put("75thPercentile", snapshot.get75thPercentile());
        data.put("95thPercentile", snapshot.get95thPercentile());
        data.put("98thPercentile", snapshot.get98thPercentile());
        data.put("99thPercentile", snapshot.get99thPercentile());
        data.put("999thPercentile", snapshot.get999thPercentile());
        data.put("durationUnit", "NANOSECONDS");
        return data;
    }

    private Map<String, Object> gauge(com.codahale.metrics.Gauge gauge){
        Map<String,Object> data = new HashMap<>();
        data.put("type", "gauge");
        data.put("value", gauge.getValue());
        return data;
    }

}

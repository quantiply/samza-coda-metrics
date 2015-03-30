package com.quantiply.samza;

import com.codahale.metrics.*;
import org.apache.samza.metrics.Gauge;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by arrawatia on 3/26/15.
 */
public class MapGauge extends Gauge<Map<String, String>> {

    private Metric _metric;

    public MapGauge(String name, Metric metric) {
        super(name, new HashMap<String, String>());
        _metric = metric;
    }

    @Override
    public Map<String, String> getValue() {
        return toMap(_metric);
    }

    private Map<String, String> toMap(Metric metric) {
        if(Meter.class.isInstance(metric)){
            return meter((Meter) metric);
        }
        else if(Histogram.class.isInstance(metric)){
            return histogram((Histogram) metric);
        }
        else if(Counter.class.isInstance(metric)){
            return counter((Counter) metric);
        }
        else if(Timer.class.isInstance(metric)){
            return timer((Timer) metric);
        }
        else if(Gauge.class.isInstance(metric)){
            return gauge((Gauge) metric);
        }

        return null;
    }

    public Map<String, String> meter(Meter meter) {
        Map<String,String> data = new HashMap<String,String>();
        data.put("count","" + meter.getCount());
        data.put("oneMinuteRate","" + meter.getOneMinuteRate());
        data.put("fiveMinuteRate","" + meter.getFiveMinuteRate());
        data.put("fifteenMinuteRate","" + meter.getFifteenMinuteRate());
        data.put("meanRate","" + meter.getMeanRate());
        return data;
    }

    private Map<String, String> counter(Counter counter) {
        Map<String,String> data = new HashMap<String,String>();
        data.put("count", "" + counter.getCount());
        return data;

    }

    private Map<String, String> histogram(Histogram histogram) {
        Map<String,String> data = new HashMap<String,String>();
        final Snapshot snapshot = histogram.getSnapshot();
        data.put("min","" + snapshot.getMin());
        data.put("max","" + snapshot.getMax());
        data.put("mean","" + snapshot.getMean());
        data.put("stdDev","" + snapshot.getStdDev());
        data.put("median","" + snapshot.getMedian());
        data.put("75thPercentile","" + snapshot.get75thPercentile());
        data.put("95thPercentile","" + snapshot.get95thPercentile());
        data.put("98thPercentile","" + snapshot.get98thPercentile());
        data.put("99thPercentile","" + snapshot.get99thPercentile());
        data.put("999thPercentile", "" + snapshot.get999thPercentile());
        return data;
    }

    private Map<String, String> timer(Timer timer) {
        Map<String,String> data = new HashMap<String,String>();
        final Snapshot snapshot = timer.getSnapshot();
        data.put("min","" + snapshot.getMin());
        data.put("max","" + snapshot.getMax());
        data.put("mean","" + snapshot.getMean());
        data.put("stdDev","" + snapshot.getStdDev());
        data.put("median","" + snapshot.getMedian());
        data.put("75thPercentile","" + snapshot.get75thPercentile());
        data.put("95thPercentile","" + snapshot.get95thPercentile());
        data.put("98thPercentile","" + snapshot.get98thPercentile());
        data.put("99thPercentile","" + snapshot.get99thPercentile());
        data.put("999thPercentile", "" + snapshot.get999thPercentile());
        return data;
    }

    private Map<String, String> gauge(Gauge<?> gauge){
        Map<String,String> data = new HashMap<String,String>();
        data.put("value", "" + gauge.getValue());
        return data;
    }

}

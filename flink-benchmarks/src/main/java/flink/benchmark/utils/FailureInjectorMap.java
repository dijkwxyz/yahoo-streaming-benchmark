package flink.benchmark.utils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class FailureInjectorMap<T> extends ProcessFunction<T, T> {
    /**
     * failure rate at this operator
     */
    private double localFailureRatePerTimeSliceMs;
    /**
     * MTTI in milliseconds for the whole application
     */
    private long globalMttiMilliSeconds;
    /**
     * parallelismof application
     */
    private int parallelism;
    private long prevTime = -1;
    private boolean first = true;
    /**
     * interval that checks for failure inejction
     */
    private long timeSliceMs = 1000;

    /**
     * @param globalMttiMs mean time to interrupt in milliseconds
     */
    public FailureInjectorMap(long globalMttiMs, int parallelism) {
        this.parallelism = parallelism;
        this.globalMttiMilliSeconds = globalMttiMs;
        this.localFailureRatePerTimeSliceMs = (double) timeSliceMs / globalMttiMs / parallelism;
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        FailureInjectorMap failureInjectorMap = new FailureInjectorMap(2000, 2);
        DescriptiveStatistics ds = new DescriptiveStatistics();
        long prev = System.currentTimeMillis();
        //get 100 failures and calculate MTTI
        while (ds.getN() < 3) {
            if (failureInjectorMap.test()) {
                long curr = System.currentTimeMillis();
                ds.addValue(curr - prev);
                prev = curr;
            }
        }
        System.out.println(ds.getMean());
    }
    /**
     * inject failure based on failure rate
     */
    public void maybeInjectFailure() {
        if (new Random().nextDouble() < getLocalFailureRatePerTimeSliceMs()) {
            throw new RuntimeException(String.format("Injecting artificial failure with global mtti %d ms, parallelism %d, time %d",
                    globalMttiMilliSeconds, parallelism, System.currentTimeMillis()));
        }
    }

    public boolean test() {
        long currTime = System.currentTimeMillis();
        long timeDiff = currTime - prevTime;
        if (timeDiff >= timeSliceMs) {
            double roll = new Random().nextDouble();
            if (roll < getLocalFailureRatePerTimeSliceMs()) {
                return true;
            }
            prevTime = currTime;
        }
        return false;
    }

    public double getLocalFailureRatePerTimeSliceMs() {
        return localFailureRatePerTimeSliceMs;
    }

    public void setFailureRate(double failureRatePerMs) {
        this.localFailureRatePerTimeSliceMs = failureRatePerMs;
    }

    @Override
    public void processElement(T t, Context context, Collector<T> collector) throws Exception {
        if (first) {
            long triggerTime = context.timerService().currentProcessingTime() + 5000L;
            context.timerService().registerProcessingTimeTimer(triggerTime);
            first = false;
        }
        collector.collect(t);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<T> out) throws Exception {
        //inject failure on timer
        maybeInjectFailure();
        long triggerTime = ctx.timerService().currentProcessingTime() + timeSliceMs;
        ctx.timerService().registerProcessingTimeTimer(triggerTime);
    }
}

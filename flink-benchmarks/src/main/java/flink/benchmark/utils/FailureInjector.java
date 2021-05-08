package flink.benchmark.utils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class FailureInjector extends KeyedProcessFunction<String, Tuple3<String, String, Long>, Tuple3<String, String, Long>> {
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
    private boolean first = true;
    public final static long BASE_TIME_SLICE_MS = 100;
    /**
     * interval that checks for failure inejction
     */
    private long timeSliceMs;

    /**
     * @param globalMttiMs globalMttiMs mean time to interrupt in milliseconds
     * @param numKeys Flink timers are resigered per key.
     *                We adjust the timeSlice based on numKeys to control the MTTI.
     */
    public FailureInjector(long globalMttiMs, int numKeys) {
        this.parallelism = numKeys;
        this.globalMttiMilliSeconds = globalMttiMs;
        this.localFailureRatePerTimeSliceMs = (double) BASE_TIME_SLICE_MS / globalMttiMs;
        this.timeSliceMs = numKeys * BASE_TIME_SLICE_MS;
    }


    /**
     * @param args
     */
    public static void main(String[] args) {
        FailureInjector failureInjector = new FailureInjector(2000, 2);
        DescriptiveStatistics ds = new DescriptiveStatistics();
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

    public double getLocalFailureRatePerTimeSliceMs() {
        return localFailureRatePerTimeSliceMs;
    }

    public void setFailureRate(double failureRatePerMs) {
        this.localFailureRatePerTimeSliceMs = failureRatePerMs;
    }


    @Override
    public void processElement(Tuple3<String, String, Long> t, Context context, Collector<Tuple3<String, String, Long>> collector) throws Exception {
        if (first) {
            long triggerTime = context.timerService().currentProcessingTime() + timeSliceMs;
            context.timerService().registerProcessingTimeTimer(triggerTime);
            first = false;
        }
        collector.collect(t);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
        //inject failure on timer
        maybeInjectFailure();
        long triggerTime = ctx.timerService().currentProcessingTime() + timeSliceMs;
        ctx.timerService().registerProcessingTimeTimer(triggerTime);
    }
}

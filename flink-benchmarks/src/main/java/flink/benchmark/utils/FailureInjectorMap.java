package flink.benchmark.utils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FailureInjectorMap<T> extends RichMapFunction<T, T> {
    /**
     * failure rate at this operator
     */
    private double localFailureRatePerMs;
    /**
     * MTTI in milliseconds for the whole application
     */
    private long globalMttiMilliSeconds;
    /**
     * parallelismof application
     */
    private int parallelism;
    private long prevTime = -1;
    private boolean injectFailures;

    private long startTimeDelayMs;
    private long startTimeMs;
//    private ListState<Long> startTimeMsState;
    /**
     * @param globalMttiMs mean time to interrupt in milliseconds
     */
    public FailureInjectorMap(long globalMttiMs, int parallelism, long startTimeDelayMs) throws IOException {
        this.parallelism = parallelism;
        this.globalMttiMilliSeconds = globalMttiMs;
        this.localFailureRatePerMs = 1.0 / globalMttiMs / parallelism;
        this.injectFailures = globalMttiMs > 0;
        this.startTimeDelayMs = startTimeDelayMs;
        this.startTimeMs = startTimeDelayMs + System.currentTimeMillis();
        System.out.println("Inject Software Failures: " + injectFailures);
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {
        FailureInjectorMap failureInjectorMap = new FailureInjectorMap(100, 1, 0L);
        DescriptiveStatistics ds = new DescriptiveStatistics();
        long prev = System.currentTimeMillis();
        //get 100 failures and calculate MTTI
        while (ds.getN() < 100) {
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
        long currTime = System.currentTimeMillis();
        if (prevTime < 0) {
            prevTime = currTime;
        }
        if (currTime - prevTime > 0) {
            double roll = new Random().nextDouble();
            if (roll < (currTime - prevTime) * getLocalFailureRatePerMs()) {
                startTimeMs = startTimeDelayMs + System.currentTimeMillis();
                throw new RuntimeException(String.format("Injecting artificial failure with global mtti %d ms, parallelism %d, time slice %d ms, timestamp %d",
                        globalMttiMilliSeconds, parallelism, currTime - prevTime, currTime));
            }
            prevTime = currTime;
        }
    }

    public boolean test() {
        long currTime = System.currentTimeMillis();
        long timeDiff = currTime - prevTime;
        if (timeDiff > 0) {
            double roll = new Random().nextDouble();
            if (roll < timeDiff * getLocalFailureRatePerMs()) {
                return true;
            }
            prevTime = currTime;
        }
        return false;
    }

    public double getLocalFailureRatePerMs() {
        return localFailureRatePerMs;
    }

    public void setFailureRate(double failureRatePerMs) {
        this.localFailureRatePerMs = failureRatePerMs;
    }

    @Override
    public T map(T value) throws Exception {
        if (System.currentTimeMillis() > startTimeMs && injectFailures) {
            maybeInjectFailure();
        }
        return value;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        startTimeMs = System.currentTimeMillis() + startTimeDelayMs;
    }

    //    @Override
//    public void snapshotState(FunctionSnapshotContext context) throws Exception {
//    }
//
//    @Override
//    public void initializeState(FunctionInitializationContext context) throws Exception {
//
//        ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("startTimeMs", Long.class);
//        this.startTimeMsState = context.getOperatorStateStore().getListState(descriptor);
//
//        startTimeMs = System.currentTimeMillis() + startTimeDelayMs;
//
//        startTimeMsState.clear();
//        ArrayList<Long> arr = new ArrayList<>();
//        arr.add(startTimeMs);
//        this.startTimeMsState.update(arr);
//    }
}

package flink.benchmark.utils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.Random;

public class FailureInjectorMap<T> extends RichMapFunction<T, T> {
    /**
     * failure rate at this operator
     */
    private double localFailureRatePerSec;
    /**
     * MTTI in milliseconds for the whole application
     */
    private long globalMttiMilliSeconds;
    private final boolean injectWithProbability;
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
    public FailureInjectorMap(long globalMttiMs, boolean injectWithProbability, int parallelism, long startTimeDelayMs) throws IOException {
        this.parallelism = parallelism;
        this.globalMttiMilliSeconds = globalMttiMs;
        this.injectWithProbability = injectWithProbability;
        this.localFailureRatePerSec = 1000.0 / globalMttiMs / parallelism;
        this.injectFailures = globalMttiMs > 0;
        this.startTimeDelayMs = startTimeDelayMs;
        this.startTimeMs = startTimeDelayMs + System.currentTimeMillis();
        System.out.println("Inject Software Failures: " + injectFailures);
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        long prev = System.currentTimeMillis();
        System.out.println(prev);
        FailureInjectorMap failureInjectorMap = new FailureInjectorMap<Integer>(10000, true, 1, 0L);
        DescriptiveStatistics ds = new DescriptiveStatistics();
//        while (true) {
//            failureInjectorMap.map(1);
//        }
        // get 100 failures and calculate MTTI
        int size = 10;
        long[] res = new long[size];
        int ct = 0;
        while (ct < size) {
            try {
                failureInjectorMap.map(1);
            } catch (RuntimeException e) {
                long curr = System.currentTimeMillis();
                res[ct] = curr - prev;
                prev = curr;
                ct++;
                failureInjectorMap = new FailureInjectorMap<Integer>(10000, true, 1, 0L);
            }
        }

        long sum = 0;
        for (long l : res) {
            sum += l;
        }

        System.out.println((double) sum / res.length);
    }

    /**
     * inject failure based on failure rate
     */
    public void maybeInjectFailure() {
        long currTime = System.currentTimeMillis();
        if (prevTime < 0) {
            prevTime = currTime;
        }
        if (currTime - prevTime > 100) {
            double roll = new Random().nextDouble();
            if (roll < (double) ((currTime - prevTime) / 1000) * getLocalFailureRatePerSec()) {
                startTimeMs = startTimeDelayMs + System.currentTimeMillis();
                injectFailure(currTime);
            }
            prevTime = currTime;
        }
    }

    private void injectFailure(long currTime) {
        throw new RuntimeException(String.format("Injecting artificial failure with global mtti %d ms, parallelism %d, time slice %d ms, timestamp %d",
                globalMttiMilliSeconds, parallelism, currTime - prevTime, currTime));
    }

    public boolean test() {
        long currTime = System.currentTimeMillis();
        long timeDiff = currTime - prevTime;
        if (timeDiff > 0) {
            double roll = new Random().nextDouble();
            if (roll < timeDiff * getLocalFailureRatePerSec()) {
                return true;
            }
            prevTime = currTime;
        }
        return false;
    }

    public double getLocalFailureRatePerSec() {
        return localFailureRatePerSec;
    }

    public void setFailureRate(double failureRatePerMs) {
        this.localFailureRatePerSec = failureRatePerMs;
    }

    @Override
    public T map(T value) throws Exception {
        if (!injectFailures) {
            return value;
        }

        if (injectWithProbability) {
            if (System.currentTimeMillis() > startTimeMs) {
                maybeInjectFailure();
            }
        } else {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis > startTimeMs + globalMttiMilliSeconds) {
                injectFailure(currentTimeMillis);
            }
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

package flink.benchmark.utils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Random;

public class FailureInjectorMap<T> implements MapFunction<T, T> {
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

    /**
     * @param globalMttiMs mean time to interrupt in milliseconds
     */
    public FailureInjectorMap(long globalMttiMs, int parallelism) {
        this.parallelism = parallelism;
        this.globalMttiMilliSeconds = globalMttiMs;
        this.localFailureRatePerMs = 1.0 / globalMttiMs / parallelism;
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        FailureInjectorMap failureInjectorMap = new FailureInjectorMap(100, 1);
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
                throw new RuntimeException(String.format("Injecting artificial failure with global mtti %d ms, parallelism %d, from time %d to %d",
                        globalMttiMilliSeconds, parallelism, prevTime, currTime));
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
    public T map(T t) throws Exception {
        maybeInjectFailure();
        return t;
    }
}

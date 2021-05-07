package flink.benchmark.utils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Random;

public class FailureInjectorMap<T> implements MapFunction<T, T> {
    private final Random random = new Random();
    private double failureRatePerMs;
    private long prevTime = System.currentTimeMillis();

    /**
     * @param mttiMilliSeconds mean time to interrupt in milliseconds
     */
    public FailureInjectorMap(double mttiMilliSeconds) {
        this.failureRatePerMs = 1.0 / mttiMilliSeconds;
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        FailureInjectorMap failureInjectorMap = new FailureInjectorMap(100);
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
        if (currTime - prevTime > 0) {
            double roll = random.nextDouble();
            if (roll < (currTime - prevTime) * getFailureRatePerMs()) {
                throw new RuntimeException(String.format("Injecting artificial failure at rate %f, time %d", failureRatePerMs, currTime));
            }
            prevTime = currTime;
        }
    }

    public boolean test() {
        long currTime = System.currentTimeMillis();
        long timeDiff = currTime - prevTime;
        if (timeDiff > 0) {
            double roll = random.nextDouble();
            if (roll < timeDiff * getFailureRatePerMs()) {
                return true;
            }
            prevTime = currTime;
        }
        return false;
    }

    public double getFailureRatePerMs() {
        return failureRatePerMs;
    }

    public void setFailureRate(double failureRatePerMs) {
        this.failureRatePerMs = failureRatePerMs;
    }

    @Override
    public T map(T t) throws Exception {
        maybeInjectFailure();
        return t;
    }
}

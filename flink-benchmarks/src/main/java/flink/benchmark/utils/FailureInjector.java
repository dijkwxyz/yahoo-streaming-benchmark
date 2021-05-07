package flink.benchmark.utils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Random;

public class FailureInjector {
    private final Random random = new Random();
    private double failureRatePerMs;
    private long prevTime = System.currentTimeMillis();

    /**
     * @param mttiMilliSecond mean time to interrupt in milliseconds
     */
    public FailureInjector(double mttiMilliSecond) {
        this.failureRatePerMs = 1.0 / mttiMilliSecond;
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        FailureInjector failureInjector = new FailureInjector(100);
        DescriptiveStatistics ds = new DescriptiveStatistics();
        long prev = System.currentTimeMillis();
        //get 100 failures and calculate MTTI
        while (ds.getN() < 100) {
            if (failureInjector.test()) {
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
}

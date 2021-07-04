package flink.benchmark.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThroughputLoggerProcessor<T> extends ProcessFunction<T, Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(ThroughputLoggerProcessor.class);

    private long totalReceived = 0;
    private long lastTotalReceived = 0;
    private long lastLogTimeMs = -1;
    private int elementSize;
    private long logfreq;

    public ThroughputLoggerProcessor(int elementSize, long logfreq) {
        this.elementSize = elementSize;
        this.logfreq = logfreq;
    }

    @Override
    public void processElement(T t, Context context, Collector<Integer> collector) throws Exception {
        totalReceived++;
        if (totalReceived % logfreq == 0) {
            // throughput over entire time
            long now = System.currentTimeMillis();

            // throughput for the last "logfreq" elements
            if (lastLogTimeMs == -1) {
                // init (the first)
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            } else {
                long timeDiff = now - lastLogTimeMs;
                long elementDiff = totalReceived - lastTotalReceived;
                double ex = (1000 / (double) timeDiff);
                // LOG.info("From {} to {} ({} ms), we received {} elements. That's {} elements/second/core. {} MB/sec/core. GB received {} for task {}",
                //         lastLogTimeMs, now, timeDiff, elementDiff, elementDiff * ex,
                //         elementDiff * ex * elementSize / 1024 / 1024,
                //         (totalReceived * elementSize) / 1024 / 1024 / 1024,
                //         getRuntimeContext().getIndexOfThisSubtask());
                System.out.println(String.format("#####%s,%s,%s,%s,%s,%s,%s,%s&&&&&",
                        lastLogTimeMs, now, timeDiff, elementDiff, elementDiff * ex,
                        elementDiff * ex * elementSize / 1024 / 1024,
                        (totalReceived * elementSize) / 1024 / 1024 / 1024,
                        getRuntimeContext().getIndexOfThisSubtask()));

                // reinit
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            }
        }
    }
}
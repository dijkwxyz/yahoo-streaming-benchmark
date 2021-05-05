package flink.benchmark.utils;

import flink.benchmark.BenchmarkConfig;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyzeTool {
    static NumberFormat nf = NumberFormat.getNumberInstance();

    static {
        nf.setMaximumFractionDigits(0);
    }

    public static class LatencyResult {
        DescriptiveStatistics eventTimeLatencies = new DescriptiveStatistics();
        DescriptiveStatistics processingTimeLatencies = new DescriptiveStatistics();
        Map<String, DescriptiveStatistics> perHostProcLat = new HashMap<>();
        Map<String, DescriptiveStatistics> perHostEventLat = new HashMap<>();
    }

    public static class ThroughputResult {
        SummaryStatistics throughputs = new SummaryStatistics();
        Map<String, SummaryStatistics> perHostThr = new HashMap<>();
    }

    public static void gatherThroughputData(String path, String host, String dstFileName) throws IOException {
        Scanner sc = new Scanner(new File(path, host + ".log"));

        FileWriter fw = new FileWriter(new File(path, dstFileName));
        Pattern dataPattern = Pattern.compile(".*#####(.+)&&&&&.*");
        fw.write("start || end || duration || num-elements || elements/second/core || MB/sec/core || GB received\n");
        while (sc.hasNextLine()) {
//            From 1618917151330 to 1618917160712 (9382 ms), we received 1000000 elements. That's 106587.08164570454 elements/second/core. 24.395846934289064 MB/sec/core. GB received 0
//            "- #####1618917151330,1618917160712,9382,1000000,106587.08164570454,24.395846934289064,0&&&&&"
            String l = sc.nextLine();
            Matcher tpMatcher = dataPattern.matcher(l);
            if (tpMatcher.matches()) {
                fw.write(tpMatcher.group(1));
                fw.write('\n');
            }
        }

        fw.close();
    }

    public static class CheckpointData {
        int id;
        /**
         * bytes
         */
        long size;
        /**
         * ms
         */
        long startTime;
        /**
         * ms
         */
        int timeCost;

        public CheckpointData(int id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return id + "," + size + "," + startTime + "," + timeCost;
        }
    }

    public static void parseRestartCost(String path, String srcFileName, String dstFileName) throws IOException, ParseException {
        Scanner sc = new Scanner(new File(path, srcFileName));

        FileWriter fw = new FileWriter(new File(path, dstFileName));
        //2021-05-05 01:12:20,739 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Window(SlidingEventTimeWindows(10000, 2000), EventAndProcessingTimeTrigger, ProcessWindowFunction$1) -> Sink: Unnamed (7/8) (2a8ccfa5cea74ecbb2481e6152acb293) switched from RUNNING to CANCELING.
        //2021-05-05 01:12:20,739 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: Kafka -> (Flat Map, Flat Map -> Filter -> Projection -> Flat Map -> Timestamps/Watermarks -> Map) (1/8) (437d49840ee23e6c9405dba8ed413e59) switched from RUNNING to CANCELING.
        Pattern cancelPattern = Pattern.compile(".*(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) INFO.*switched from RUNNING to CANCELING.*");
        //2021-05-05 01:14:27,830 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Window(SlidingEventTimeWindows(10000, 2000), EventAndProcessingTimeTrigger, ProcessWindowFunction$1) -> Sink: Unnamed (8/8) (276a4225d3c078389e1a21c7b0e4c8e8) switched from DEPLOYING to RUNNING.
        Pattern restartPattern = Pattern.compile(".*(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) INFO.*switched from DEPLOYING to RUNNING.*");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
        ArrayList<Tuple2<Date, Integer>> arr = new ArrayList<>();
        final int toCanceled = 0;
        final int toRunning = 1;
        while (sc.hasNextLine()) {
            String l = sc.nextLine();
            Matcher cancelMatcher = cancelPattern.matcher(l);
            Matcher restartMatcher = restartPattern.matcher(l);
            if (cancelMatcher.matches()) {
                //Triggering checkpoint 1 (type=CHECKPOINT) @ 1618917145019 for job 73b8361e88c2073a9940f12ead6955cb.
                Date Date = dateFormat.parse(cancelMatcher.group(1));
                arr.add(new Tuple2<>(Date, toCanceled));
            } else if (restartMatcher.matches()) {
                //Triggering checkpoint 1 (type=CHECKPOINT) @ 1618917145019 for job 73b8361e88c2073a9940f12ead6955cb.
                Date Date = dateFormat.parse(restartMatcher.group(1));
                arr.add(new Tuple2<>(Date, toRunning));
            }
        }

        int i = 0;
//        ArrayList<Long> restartCosts = new ArrayList<>();
        //skip normal start
        while (i < arr.size() && arr.get(i).f1 == toRunning) {
            i++;
        }
        while (i < arr.size()) {
            Date cancelTime;
            Date restartTime;

            //take the first toCancel tiemstamp
            cancelTime = arr.get(i).f0;
            i++;
            //skip the rest
            while (i < arr.size() && arr.get(i).f1 == toCanceled) {
                i++;
            }
            if (i >= arr.size()) {
                break;
            }

            //take the last toRunning record
            Tuple2<Date, Integer> prevRecord = arr.get(i);
            i++;
            while (i < arr.size() && arr.get(i).f1 == toRunning) {
                prevRecord = arr.get(i);
                i++;
            }
            restartTime = prevRecord.f0;

//            restartCosts.add(restartTime.getTime() - cancelTime.getTime());
            fw.write(String.valueOf(restartTime.getTime() - cancelTime.getTime()));
            fw.write('\n');
        }

        sc.close();
        fw.close();
    }


    public static void parseCheckpoint(String path, String srcFileName, String dstFileName) throws IOException {
        Scanner sc = new Scanner(new File(path, srcFileName));

        FileWriter fw = new FileWriter(new File(path, dstFileName));
        Pattern triggerPattern = Pattern.compile(".*Triggering checkpoint (\\d) \\(type=CHECKPOINT\\) @ (\\d+) for job (\\w+).*");
        Pattern completePattern = Pattern.compile(".*Completed checkpoint (\\d) for job (\\w+) \\((\\d+) bytes in (\\d+) ms\\).*");
        //cpId -> startTime

        HashMap<Integer, CheckpointData> idToData = new HashMap<>();
        while (sc.hasNextLine()) {
            String l = sc.nextLine();
            Matcher triggerMatcher = triggerPattern.matcher(l);
            Matcher completeMatcher = completePattern.matcher(l);
            if (triggerMatcher.matches()) {
                //Triggering checkpoint 1 (type=CHECKPOINT) @ 1618917145019 for job 73b8361e88c2073a9940f12ead6955cb.
                int cpId = Integer.parseInt(triggerMatcher.group(1));
                long startTime = Long.parseLong(triggerMatcher.group(2));
                // String jobId = triggerMatcher.group(3);
                if (!idToData.containsKey(cpId)) {
                    idToData.put(cpId, new CheckpointData(cpId));
                }
                idToData.get(cpId).startTime = startTime;
            }
            if (completeMatcher.matches()) {
                //Completed checkpoint 1 for job 73b8361e88c2073a9940f12ead6955cb (6031587 bytes in 318 ms).
                int cpId = Integer.parseInt(completeMatcher.group(1));
                //                String jobId = triggerMatcher.group(2);
                long cpSize = Long.parseLong(completeMatcher.group(3));
                int timeCost = Integer.parseInt(completeMatcher.group(4));
                if (!idToData.containsKey(cpId)) {
                    idToData.put(cpId, new CheckpointData(cpId));
                }
                idToData.get(cpId).size = cpSize;
                idToData.get(cpId).timeCost = timeCost;
            }
        }

        for (int cpId : idToData.keySet()) {
            CheckpointData cpData = idToData.get(cpId);
            fw.write(cpData.toString());
            fw.write('\n');
        }

        sc.close();
        fw.close();
    }

    public static ThroughputResult analyzeThroughput(String path, String host, ThroughputResult throughputResult) throws IOException {
        Scanner sc = new Scanner(new File(path, host + ".log"));
        String l;
        Pattern throughputPattern = Pattern.compile(".*That's ([0-9.]+) elements\\/second\\/core.*");

        SummaryStatistics throughputs = new SummaryStatistics();
        while (sc.hasNextLine()) {
            l = sc.nextLine();
            Matcher tpMatcher = throughputPattern.matcher(l);
            if (tpMatcher.matches()) {
                double eps = Double.valueOf(tpMatcher.group(1));
                throughputs.addValue(eps);
                throughputResult.throughputs.addValue(eps);
            }
        }

        throughputResult.perHostThr.put(host, throughputs);

        return throughputResult;
    }

    public static LatencyResult analyzeLatency(String path, LatencyResult latencyResult) throws FileNotFoundException {
        Scanner sc = new Scanner(new File(path, "count-latency.txt"));
        while (sc.hasNextLine()) {
            String[] l = sc.nextLine().split(" ");
            int count = Integer.parseInt(l[0]);
            long eventTimeLatency = Long.parseLong(l[1]);
            long processingTimeLatency = Long.parseLong(l[2]);
            String subtask = String.valueOf(Integer.parseInt(l[3]));
            latencyResult.eventTimeLatencies.addValue(eventTimeLatency);
            latencyResult.processingTimeLatencies.addValue(processingTimeLatency);

            if (!latencyResult.perHostEventLat.containsKey(subtask)) {
                latencyResult.perHostEventLat.put(subtask, new DescriptiveStatistics());
            }
            if (!latencyResult.perHostProcLat.containsKey(subtask)) {
                latencyResult.perHostProcLat.put(subtask, new DescriptiveStatistics());
            }
            latencyResult.perHostEventLat.get(subtask).addValue(eventTimeLatency);
            latencyResult.perHostProcLat.get(subtask).addValue(processingTimeLatency);

        }

        return latencyResult;
    }

    public static void writeLatency(LatencyResult latencyResult, FileWriter statisticsWriter) throws IOException {
        DescriptiveStatistics eventTimeLatencies = latencyResult.eventTimeLatencies;
        DescriptiveStatistics processingTimeLatencies = latencyResult.processingTimeLatencies;

        StringBuilder sb = new StringBuilder();
        sb.append("====== " + "all-machines" + " =======");
        sb.append('\n');
        sb.append("lat-mean||lat-median||lat-90percentile||lat-95percentile||lat-99percentile||lat-min||lat-max||num-latencies");
        sb.append('\n');
        sb.append(nf.format(eventTimeLatencies.getMean())).append("||");
        sb.append(nf.format(eventTimeLatencies.getPercentile(50))).append("||");
        sb.append(nf.format(eventTimeLatencies.getPercentile(90))).append("||");
        sb.append(nf.format(eventTimeLatencies.getPercentile(95))).append("||");
        sb.append(nf.format(eventTimeLatencies.getPercentile(99))).append("||");
        sb.append(nf.format(eventTimeLatencies.getMin())).append("||");
        sb.append(nf.format(eventTimeLatencies.getMax())).append("||");
        sb.append(nf.format(eventTimeLatencies.getN()));
        sb.append('\n');
        sb.append(nf.format(processingTimeLatencies.getMean())).append("||");
        sb.append(nf.format(processingTimeLatencies.getPercentile(50))).append("||");
        sb.append(nf.format(processingTimeLatencies.getPercentile(90))).append("||");
        sb.append(nf.format(processingTimeLatencies.getPercentile(95))).append("||");
        sb.append(nf.format(processingTimeLatencies.getPercentile(99))).append("||");
        sb.append(nf.format(processingTimeLatencies.getMin())).append("||");
        sb.append(nf.format(processingTimeLatencies.getMax())).append("||");
        sb.append(nf.format(processingTimeLatencies.getN()));
        sb.append('\n');
        String str = sb.toString();
        statisticsWriter.write(str);
        System.out.println(str);


        sb = new StringBuilder();
        for (String key : latencyResult.perHostEventLat.keySet()) {
            DescriptiveStatistics eventTime = latencyResult.perHostEventLat.get(key);
            sb.append("============== ").append(key).append(" (entries: ").append(eventTime.getN()).append(") ===============");
            DescriptiveStatistics procTime = latencyResult.perHostProcLat.get(key);
            sb.append('\n');
            sb.append("Mean event-time latency:   ").append(nf.format(eventTime.getMean()));
            sb.append("      || ");
            sb.append("Mean processing-time latency:   ").append(nf.format(procTime.getMean()));
            sb.append('\n');
            sb.append("Median event-time latency: ").append(nf.format(eventTime.getPercentile(50)));
            sb.append("      || ");
            sb.append("Median processing-time latency: ").append(nf.format(procTime.getPercentile(50)));
            sb.append('\n');
        }
        str = sb.toString();
        statisticsWriter.write(str);
        System.out.println(str);

    }

    public static void writeThroughput(ThroughputResult throughputResult, FileWriter fw) throws IOException {
        SummaryStatistics throughputs = throughputResult.throughputs;
        StringBuilder sb = new StringBuilder();
        sb.append("====== " + "all-machines" + " =======");
        sb.append('\n');
        sb.append("throughput-mean||throughput-max||throughputs");
        sb.append('\n');
        sb.append(nf.format(throughputs.getMean())).append("||");
        sb.append(nf.format(throughputs.getMax())).append("||");
        sb.append(nf.format(throughputs.getN()));
        sb.append('\n');
        String str = sb.toString();
        fw.write(str);
        System.out.println(str);

        sb = new StringBuilder();
        sb.append("================= Throughput (in total " + throughputResult.perHostThr.size() + " reports ) =====================");
        sb.append('\n');
        for (Map.Entry<String, SummaryStatistics> entry : throughputResult.perHostThr.entrySet()) {
            sb.append("====== ").append(entry.getKey()).append(" (entries: ").append(entry.getValue().getN()).append(")=======");
            sb.append('\n');
            sb.append("Mean throughput: ").append(nf.format(entry.getValue().getMean()));
            sb.append('\n');
        }
        str = sb.toString();
        fw.write(str);
        System.out.println(str);

    }

    public static void writeLatencyThroughput(LatencyResult latencyResult, ThroughputResult throughputResult, String path, String fileName) throws IOException {
        FileWriter fw = new FileWriter(new File(path, fileName));
        writeLatency(latencyResult, fw);
        writeThroughput(throughputResult, fw);
        fw.close();
    }

    public static void copyFile(String srcDir, String dstDir, String fileName) throws IOException {
        FileChannel inputChannel = null;
        FileChannel outputChannel = null;
        inputChannel = new FileInputStream(
                new File(srcDir, fileName))
                .getChannel();
        outputChannel = new FileOutputStream(
                new File(dstDir, fileName))
                .getChannel();
        outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
        inputChannel.close();
        outputChannel.close();
    }

    public static void main(String[] args) throws IOException, ParseException {
        /*
         path prefix
         hostname
         */
        String dir = args[0];
        BenchmarkConfig config = new BenchmarkConfig(new File(dir, "conf-copy.yaml").getAbsolutePath());
        String load = String.valueOf(config.loadTargetHz);
        System.out.println("load = " + load);
        String date = new SimpleDateFormat("MM-dd_HH-mm-ss").format(new Date());//设置日期格式
        String generatedPrefix = date + "_load-" + load + "/";
        File generatedDir = new File(dir, generatedPrefix);
        if (!generatedDir.exists()) {
            generatedDir.mkdir();
        }

        copyFile(dir, generatedDir.getAbsolutePath(), "conf-copy.yaml");
        copyFile(dir, generatedDir.getAbsolutePath(), "count-latency.txt");

        parseRestartCost(dir, "jm.log", generatedPrefix + "restart-cost.txt");
        parseCheckpoint(dir, "jm.log", generatedPrefix + "checkpoint.txt");

        LatencyResult latencyResult = new LatencyResult();
        ThroughputResult throughputResult = new ThroughputResult();
        analyzeLatency(dir, latencyResult);
        for (int i = 1; i < args.length; i++) {
            analyzeThroughput(dir, args[i], throughputResult);
            gatherThroughputData(dir, args[i], generatedPrefix + args[i] + "_throughput.txt");
        }

        writeLatencyThroughput(latencyResult, throughputResult, dir, generatedPrefix + "latency_throughput.txt");
    }


}


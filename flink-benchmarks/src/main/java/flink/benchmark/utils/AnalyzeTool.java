package flink.benchmark.utils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyzeTool {

    public static class Result {

        DescriptiveStatistics latencies = new DescriptiveStatistics();
        ;
        SummaryStatistics throughputs = new SummaryStatistics();
        ;
        Map<String, DescriptiveStatistics> perHostLat = new HashMap<>();
        Map<String, SummaryStatistics> perHostThr = new HashMap<>();

    }

    public static void gatherThroughputData(String path, String host, String dstFileName) throws IOException {
        Scanner sc = new Scanner(new File(path, host + ".log"));

        FileWriter fw = new FileWriter(new File(path, dstFileName));
        Pattern dataPattern = Pattern.compile(".*#####(.+)&&&&&.*");
        while (sc.hasNextLine()) {
//            "2021-04-20 11:12:40,713 INFO  flink.benchmark.utils.ThroughputLogger                       [] - #####1618917151330,1618917160712,9382,1000000,106587.08164570454,24.395846934289064,0&&&&&"
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
        long size;
        long startTime;
        int timeCost;

        public CheckpointData(int id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return id + "," + size + "," + startTime + "," + timeCost;
        }
    }

    public static void parseCheckpoint(String path, String srcFileName, String dstFileName) throws IOException {
        Scanner sc = new Scanner(new File(path,srcFileName));

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

        fw.close();
    }

    public static Result analyzeThroughput(String path, String host, Result result) throws IOException {
        Scanner sc = new Scanner(new File(path,host+".log"));
        String l;
        Pattern throughputPattern = Pattern.compile(".*That's ([0-9.]+) elements\\/second\\/core.*");

        SummaryStatistics throughputs = new SummaryStatistics();
        while (sc.hasNextLine()) {
            l = sc.nextLine();
            Matcher tpMatcher = throughputPattern.matcher(l);
            if (tpMatcher.matches()) {
                double eps = Double.valueOf(tpMatcher.group(1));
                throughputs.addValue(eps);
                result.throughputs.addValue(eps);
            }
        }

        result.perHostThr.put(host, throughputs);

        return result;
    }

    public static Result analyzeLatency(String path, Result result) throws FileNotFoundException {
        Scanner sc = new Scanner(new File(path, "seen-updated-subtask.txt"));
        DescriptiveStatistics latencies = new DescriptiveStatistics();
        while (sc.hasNextLine()) {
            String[] l = sc.nextLine().split(" ");
            int count = Integer.parseInt(l[0]);
            int latency = Integer.parseInt(l[1]);
            String subtask = String.valueOf(Integer.parseInt(l[2]));
            latencies.addValue(latency);
            result.latencies.addValue(latency);

            if (!result.perHostLat.containsKey(subtask)) {
                result.perHostLat.put(subtask, latencies);
            }
        }

        return result;
    }

    public static void writeLatencyThroughput(Result result, String path, String fileName) throws IOException {
        SummaryStatistics throughputs = result.throughputs;
        DescriptiveStatistics latencies = result.latencies;
        FileWriter fw = new FileWriter(new File(path, fileName));
        fw.write("====== " + "all-machines" + " =======");
        fw.write('\n');
        fw.write("lat-mean;lat-median;lat-90percentile;lat-95percentile;lat-99percentile;throughput-mean;throughput-max;latencies;throughputs;");
        fw.write('\n');
        fw.write(latencies.getMean() + ";" + latencies.getPercentile(50) + ";" + latencies.getPercentile(90) + ";" + latencies.getPercentile(95) + ";" + latencies.getPercentile(99) + ";" + throughputs.getMean() + ";" + throughputs.getMax() + ";" + latencies.getN() + ";" + throughputs.getN());
        fw.write('\n');

        for (Map.Entry<String, DescriptiveStatistics> entry : result.perHostLat.entrySet()) {
            fw.write("====== " + entry.getKey() + " (entries: " + entry.getValue().getN() + ") =======");
            fw.write('\n');
            fw.write("Mean latency " + entry.getValue().getMean());
            fw.write('\n');
            fw.write("Median latency " + entry.getValue().getPercentile(50));
            fw.write('\n');
        }

        fw.write("================= Throughput (in total " + result.perHostThr.size() + " reports ) =====================");
        fw.write('\n');
        for (Map.Entry<String, SummaryStatistics> entry : result.perHostThr.entrySet()) {
            fw.write("====== " + entry.getKey() + " (entries: " + entry.getValue().getN() + ")=======");
            fw.write('\n');
            fw.write("Mean throughput " + entry.getValue().getMean());
            fw.write('\n');
        }

        fw.close();

    }

    public static void writeLoadInfo(String path, String dstDir) throws IOException {
        Scanner sc = new Scanner(new File(path, "load.log"));
        if (sc.hasNextLine()) {
            String l = sc.nextLine().trim();
            FileWriter fw = new FileWriter(new File(path, dstDir + l));
            fw.close();
        }
    }


    public static void main(String[] args) throws IOException {
        /*
         path prefix
         hostname
         */
        args = new String[]{
                "C:\\Users\\joinp\\Downloads\\",
                "flink3"
        };
        String prefix = args[0];
        String date = new SimpleDateFormat("MM-dd_HH-mm-ss").format(new Date());//设置日期格式
        String generatedPrefix = date + "/";
        File generatedDir = new File(prefix, generatedPrefix);
        if (!generatedDir.exists()) {
            generatedDir.mkdir();
        }

        writeLoadInfo(prefix, generatedPrefix);

        parseCheckpoint(prefix, "jm.log",  generatedPrefix + "checkpoint.txt");

        Result result = new Result();
        analyzeLatency(prefix, result);
        for (int i = 1; i < args.length; i++) {
            analyzeThroughput(prefix, args[i], result);
            gatherThroughputData(prefix, args[i], generatedPrefix + args[i] + "_throughput.txt");
        }

        writeLatencyThroughput(result, prefix, generatedPrefix + "latency_throughput.txt");
    }


}


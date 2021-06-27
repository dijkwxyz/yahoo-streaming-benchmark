package flink.benchmark.utils;

import flink.benchmark.BenchmarkConfig;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.java.tuple.Tuple4;

import java.io.*;
import java.nio.channels.FileChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyzeTool {

    public static class LatencyResult {
        DescriptiveStatistics eventTimeLatencies = new DescriptiveStatistics();
        Map<String, DescriptiveStatistics> perHostEventLat = new HashMap<>();
//        DescriptiveStatistics processingTimeLatencies = new DescriptiveStatistics();
//        Map<String, DescriptiveStatistics> perHostProcLat = new HashMap<>();
    }

    public static class ThroughputResult {
        DescriptiveStatistics throughputs = new DescriptiveStatistics();
        Map<String, DescriptiveStatistics> perHostThr = new HashMap<>();
    }

    public static void gatherThroughputData(String logFileName, FileWriter fw) throws IOException {
        File file = new File(logFileName);
        if (!file.exists()) {
            return;
        }
        Scanner sc = new Scanner(file);
        Pattern dataPattern = Pattern.compile(".*#####(.+)&&&&&.*");
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
        sc.close();
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
            return id + " " + size + " " + startTime + " " + timeCost;
        }
    }

    public static void parseRestartCost(String srcFileName, String path, String dstFileName) throws IOException, ParseException {
        Scanner sc = new Scanner(new File(srcFileName));

        FileWriter fw = new FileWriter(new File(path, dstFileName));
        String timePattern = ".*(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3} \\w{3})";
        //2021-05-05 04:52:22,719 UTC INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: Kafka -> (Flat Map, Flat Map -> Filter -> Projection -> Flat Map -> Timestamps/Watermarks -> Map) (8/8) (9530062f70342991c61311d3d5f47eba) switched from RUNNING to FAILED on org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot@2b36afee.
        Pattern failedPattern = Pattern.compile(timePattern + " .*switched from \\w+ to FAILED.*");
        //2021-05-07 02:02:32,878 UTC INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Job WordCount Global Window Experiment (c405f119755983293e2309850970b3a0) switched from state RUNNING to RESTARTING.
        Pattern restartPattern = Pattern.compile(timePattern + " .*to RESTARTING.*");
        //2021-05-05 01:14:24,424 UTC INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Restoring job effa6bd99425bf0381a8a567c71e016b from latest valid checkpoint: Checkpoint 81 @ 1620177217703 for effa6bd99425bf0381a8a567c71e016b.
        //2021-05-05 01:14:27,708 UTC INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Restoring job effa6bd99425bf0381a8a567c71e016b from latest valid checkpoint: Checkpoint 88 @ 1620177217703 for effa6bd99425bf0381a8a567c71e016b.
        Pattern loadCheckpointPattern = Pattern.compile(timePattern + " .*Restoring job \\w+ from latest valid checkpoint: Checkpoint (\\d+) @ \\d+ for \\w+.*");
        //2021-05-07 10:46:36,209 UTC INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - No checkpoint found during restore.
        Pattern noCheckpointPattern = Pattern.compile(timePattern + " .*No checkpoint found during restore.*");

        //2021-05-05 01:14:27,830 UTC INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Window(SlidingEventTimeWindows(10000, 2000), EventAndProcessingTimeTrigger, ProcessWindowFunction$1) -> Sink: Unnamed (8/8) (276a4225d3c078389e1a21c7b0e4c8e8) switched from DEPLOYING to RUNNING.
        Pattern toRunningPattern = Pattern.compile(timePattern + " INFO.*switched from DEPLOYING to RUNNING.*");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zzz");
        //(timestamp, type, info, matched string)
        ArrayList<Tuple4<Date, Integer, String, String>> arr = new ArrayList<>();
        //define type constants
        final int toFailed = 0;
        final int toRestart = 1;
        final int loadCheckpoint = 2;
        final int toRunning = 3;
        final int noCheckpoint = 4;

        DescriptiveStatistics failedDS = new DescriptiveStatistics();
        ArrayList<Date> failedArr = new ArrayList<>();
        while (sc.hasNextLine()) {
            String l = sc.nextLine();
            Matcher failedMatcher = failedPattern.matcher(l);
            Matcher restartMatcher = restartPattern.matcher(l);
            Matcher loadCheckpointMatcher = loadCheckpointPattern.matcher(l);
            Matcher noCheckpointMatcher = noCheckpointPattern.matcher(l);
            Matcher toRunningMatcher = toRunningPattern.matcher(l);
            if (failedMatcher.matches()) {
                Date date = dateFormat.parse(failedMatcher.group(1));
                arr.add(new Tuple4<>(date, toFailed, null, failedMatcher.group()));
                failedArr.add(date);
            } else if (restartMatcher.matches()) {
                Date date = dateFormat.parse(restartMatcher.group(1));
                arr.add(new Tuple4<>(date, toRestart, null, restartMatcher.group()));
            } else if (loadCheckpointMatcher.matches()) {
                Date date = dateFormat.parse(loadCheckpointMatcher.group(1));
                arr.add(new Tuple4<>(date, loadCheckpoint, loadCheckpointMatcher.group(2), loadCheckpointMatcher.group()));
            }else if (noCheckpointMatcher.matches()) {
                Date date = dateFormat.parse(noCheckpointMatcher.group(1));
                arr.add(new Tuple4<>(date, noCheckpoint, null, noCheckpointMatcher.group()));
            } else if (toRunningMatcher.matches()) {
                Date date = dateFormat.parse(toRunningMatcher.group(1));
                arr.add(new Tuple4<>(date, toRunning, null, toRunningMatcher.group()));
            }
        }

        for (int f = 1; f < failedArr.size(); f++) {
            long timeDiff = failedArr.get(f).getTime() - failedArr.get(f - 1).getTime();
            failedDS.addValue(timeDiff);
        }


        fw.write(String.format("MTTI: %f, total failures: %d\n", failedDS.getMean(), 1 + failedDS.getN()));

        int i = 0;
        //skip normal start
        while (i < arr.size() && arr.get(i).f1 == toRunning) {
            i++;
        }

        fw.write("checkpointId failedToRestart restartToLoadCP loadCPToRunning");
        fw.write(' ');
        fw.write("failedTime restartTime loadCheckpointTime toRunningTime");
        fw.write('\n');

        while (i < arr.size()) {
            Date failedTime;
            Date restartTime;
            Date toRunningTime;
            Date loadCheckpointTime;
            String checkpointId;

            //skip state before first failure
            while(i < arr.size() && arr.get(i).f1 != toFailed) {
                i++;
            }
            if (i >= arr.size()) {
                break;
            }

            failedTime = arr.get(i).f0;
            i++;
            if (i >= arr.size()) {
                break;
            }

            assert (arr.get(i).f1 == toRestart);
            restartTime = arr.get(i).f0;
            i++;
            if (i >= arr.size()) {
                break;
            }

            assert (arr.get(i).f1 == loadCheckpoint || arr.get(i).f1 == noCheckpoint);
            loadCheckpointTime = arr.get(i).f0;
            checkpointId = arr.get(i).f1 == loadCheckpoint ? arr.get(i).f2 : "-1";
            i++;
            if (i >= arr.size()) {
                break;
            }

            //take the last toRunning record
            Tuple4<Date, Integer, String, String> prevRecord = arr.get(i);
            i++;
            while (i < arr.size() && arr.get(i).f1 == toRunning) {
                prevRecord = arr.get(i);
                i++;
            }
            toRunningTime = prevRecord.f0;


            fw.write(checkpointId);

            fw.write(' ');
            fw.write(String.valueOf(restartTime.getTime() - failedTime.getTime()));
            fw.write(' ');
            fw.write(String.valueOf(loadCheckpointTime.getTime() - restartTime.getTime()));
            fw.write(' ');
            fw.write(String.valueOf(toRunningTime.getTime() - loadCheckpointTime.getTime()));

            fw.write(' ');
            fw.write(String.valueOf(failedTime.getTime()));
            fw.write(' ');
            fw.write(String.valueOf(restartTime.getTime()));
            fw.write(' ');
            fw.write(String.valueOf(loadCheckpointTime.getTime()));
            fw.write(' ');
            fw.write(String.valueOf(toRunningTime.getTime()));

            fw.write('\n');

        }

        sc.close();
        fw.close();
    }


    public static void parseCheckpoint(String srcFileName, String path, String dstFileName) throws IOException {
        Scanner sc = new Scanner(new File(srcFileName));

        Pattern triggerPattern = Pattern.compile(".*Triggering checkpoint (\\d+) \\(type=CHECKPOINT\\) @ (\\d+) for job (\\w+).*");
        Pattern completePattern = Pattern.compile(".*Completed checkpoint (\\d+) for job (\\w+) \\((\\d+) bytes in (\\d+) ms\\).*");
        //cpId -> startTime

        HashMap<Integer, CheckpointData> idToData = new HashMap<>();
        while (sc.hasNextLine()) {
            String l = sc.nextLine();
            Matcher triggerMatcher = triggerPattern.matcher(l);
            Matcher completeMatcher = completePattern.matcher(l);
            if (triggerMatcher.matches()) {
                //Triggering checkpoint 12 (type=CHECKPOINT) @ 1618917145019 for job 73b8361e88c2073a9940f12ead6955cb.
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

        FileWriter fw = new FileWriter(new File(path, dstFileName));
        fw.write("id size_bytes startTime_ms timeCost_ms\n");
        for (int cpId : idToData.keySet()) {
            CheckpointData cpData = idToData.get(cpId);
            fw.write(cpData.toString());
            fw.write('\n');
        }

        sc.close();
        fw.close();
    }

    public static ThroughputResult analyzeThroughput(
            String inDir, String fileName,
            ThroughputResult throughputResult, FileWriter fw) throws IOException {
        Scanner sc = new Scanner(new File(inDir, fileName));
        // data format
        // start || end || duration || num-elements || elements/second/core || MB/sec/core || GB received
        // 1621437064490,1621437069120,4630,1000000,215982.7213822894,49.43452180075594,0

        // throughput = elements/second/core
        DescriptiveStatistics throughputs = new DescriptiveStatistics();
        // skip header line
        sc.nextLine();
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            fw.write(line + '\n');
            String[] l = line.split(",");
            double eps = Double.valueOf(l[4]);
            throughputs.addValue(eps);
            throughputResult.throughputs.addValue(eps);
        }

        throughputResult.perHostThr.put(fileName, throughputs);
        sc.close();
        return throughputResult;
    }

    public static LatencyResult analyzeLatency(String path, LatencyResult latencyResult) throws FileNotFoundException {
        Scanner sc = new Scanner(new File(path, "count-latency.txt"));
        while (sc.hasNextLine()) {
            String[] l = sc.nextLine().split(" ");
            long count = Long.parseLong(l[0]);
            long eventTimeLatency = Long.parseLong(l[1]);
            long processingTimeLatency = Long.parseLong(l[2]);
            String subtask = String.valueOf(Integer.parseInt(l[3]));
            latencyResult.eventTimeLatencies.addValue(eventTimeLatency);
//            latencyResult.processingTimeLatencies.addValue(processingTimeLatency);

            if (!latencyResult.perHostEventLat.containsKey(subtask)) {
                latencyResult.perHostEventLat.put(subtask, new DescriptiveStatistics());
            }
            latencyResult.perHostEventLat.get(subtask).addValue(eventTimeLatency);
//            if (!latencyResult.perHostProcLat.containsKey(subtask)) {
//                latencyResult.perHostProcLat.put(subtask, new DescriptiveStatistics());
//            }
//            latencyResult.perHostProcLat.get(subtask).addValue(processingTimeLatency);

        }
        sc.close();
        return latencyResult;
    }

    public static void writeLatencyStat(LatencyResult latencyResult, FileWriter statisticsWriter) throws IOException {
        DescriptiveStatistics eventTimeLatencies = latencyResult.eventTimeLatencies;
//        DescriptiveStatistics processingTimeLatencies = latencyResult.processingTimeLatencies;

        StringBuilder sb = new StringBuilder();
        sb.append("====== " + "all-machines" + " =======");
        sb.append('\n');
        sb.append("lat-mean,lat-median,lat-90percentile,lat-95percentile,lat-99percentile,lat-min,lat-max,num-latencies");
        sb.append('\n');
        sb.append(String.format("%.0f",eventTimeLatencies.getMean())).append(",");
        sb.append(String.format("%.0f",eventTimeLatencies.getPercentile(50))).append(",");
        sb.append(String.format("%.0f",eventTimeLatencies.getPercentile(90))).append(",");
        sb.append(String.format("%.0f",eventTimeLatencies.getPercentile(95))).append(",");
        sb.append(String.format("%.0f",eventTimeLatencies.getPercentile(99))).append(",");
        sb.append(String.format("%.0f",eventTimeLatencies.getMin())).append(",");
        sb.append(String.format("%.0f",eventTimeLatencies.getMax())).append(",");
        sb.append(String.format("%d",eventTimeLatencies.getN()));
        sb.append('\n');
//        sb.append(String.format("%.0f",processingTimeLatencies.getMean())).append(",");
//        sb.append(String.format("%.0f",processingTimeLatencies.getPercentile(50))).append(",");
//        sb.append(String.format("%.0f",processingTimeLatencies.getPercentile(90))).append(",");
//        sb.append(String.format("%.0f",processingTimeLatencies.getPercentile(95))).append(",");
//        sb.append(String.format("%.0f",processingTimeLatencies.getPercentile(99))).append(",");
//        sb.append(String.format("%.0f",processingTimeLatencies.getMin())).append(",");
//        sb.append(String.format("%.0f",processingTimeLatencies.getMax())).append(",");
//        sb.append(String.format("%.0f",processingTimeLatencies.getN()));
//        sb.append('\n');
        String str = sb.toString();
        statisticsWriter.write(str);
        System.out.println(str);


        sb = new StringBuilder();
        for (String key : latencyResult.perHostEventLat.keySet()) {
            DescriptiveStatistics eventTime = latencyResult.perHostEventLat.get(key);
//            DescriptiveStatistics procTime = latencyResult.perHostProcLat.get(key);
            sb.append("============== ").append(key).append(" (entries: ").append(eventTime.getN()).append(") ===============");
            sb.append('\n');
            sb.append("Mean event-time latency:   ").append(String.format("%.0f",eventTime.getMean()));
//            sb.append("      , ");
//            sb.append("Mean processing-time latency:   ").append(String.format("%.0f",procTime.getMean()));
            sb.append('\n');
            sb.append("Median event-time latency: ").append(String.format("%.0f",eventTime.getPercentile(50)));
//            sb.append("      , ");
//            sb.append("Median processing-time latency: ").append(String.format("%.0f",procTime.getPercentile(50)));
            sb.append('\n');
        }
        str = sb.toString();
        statisticsWriter.write(str);
        System.out.println(str);

    }

    public static void writeThroughputStat(ThroughputResult throughputResult, FileWriter fw) throws IOException {
        DescriptiveStatistics throughputs = throughputResult.throughputs;
        StringBuilder sb = new StringBuilder();
        sb.append("====== " + "all-machines" + " =======");
        sb.append('\n');
        sb.append("throughput-mean,throughput-max,throughputs");
        sb.append('\n');
        sb.append(String.format("%.0f",throughputs.getMean())).append(",");
        sb.append(String.format("%.0f",throughputs.getMax())).append(",");
        sb.append(String.format("%d",throughputs.getN()));
        sb.append('\n');
        String str = sb.toString();
        fw.write(str);
        System.out.println(str);

        sb = new StringBuilder();
        sb.append("================= Throughput (in total " + throughputResult.perHostThr.size() + " reports ) =====================");
        sb.append('\n');
        for (Map.Entry<String, DescriptiveStatistics> entry : throughputResult.perHostThr.entrySet()) {
            sb.append("====== ").append(entry.getKey()).append(" (entries: ").append(entry.getValue().getN()).append(")=======");
            sb.append('\n');
            sb.append("Mean throughput: ").append(String.format("%.0f",entry.getValue().getMean()));
            sb.append('\n');
        }
        str = sb.toString();
        fw.write(str);
        System.out.println(str);

    }

    public static void writeLatencyThroughputStat(
            LatencyResult latencyResult, ThroughputResult throughputResult,
            String outputDir, String fileName) throws IOException {
        FileWriter fw = new FileWriter(new File(outputDir, fileName));
        writeLatencyStat(latencyResult, fw);
        writeThroughputStat(throughputResult, fw);
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
        // jm outputDir logFileName
        // args = "jm C:\\Users\\46522\\Downloads\\results\\ C:\\Users\\46522\\Downloads\\results\\flink-ec2-user-standalonesession-0-multilevel-benchmark-5.novalocal.log".split(" ");
        // tm outputDir ...logFilePaths
        // args = "tm C:\\Users\\46522\\Downloads\\results\\ C:\\Users\\46522\\Downloads\\results\\flink2.log C:\\Users\\46522\\Downloads\\results\\flink3.log".split(" ");
        // zk resultDir ...tmFileNames
        // args = "zk C:\\Users\\46522\\Downloads\\results flink2.txt flink3.txt".split(" ");
        int argIdx = 0;
        String mode = args[argIdx++];
        String dir = args[argIdx++];
        String fileName;
        LatencyResult latencyResult = new LatencyResult();
        ThroughputResult throughputResult = new ThroughputResult();
        switch (mode) {
            case "zk":
                // zk resultDir ...tmFileNames
                BenchmarkConfig config = new BenchmarkConfig(new File(dir, "conf-copy.yaml").getAbsolutePath());
                String load = String.valueOf(config.loadTargetHz);
                System.out.println("load = " + load);
                String date = new SimpleDateFormat("MM-dd_HH-mm-ss").format(new Date());
                String generatedPrefix = date + "_load-" + load + "/";
                File generatedDir = new File(dir, generatedPrefix);
                if (!generatedDir.exists()) {
                    generatedDir.mkdir();
                }
                String outDirAbsPath = generatedDir.getAbsolutePath();
                copyFile(dir, outDirAbsPath, "conf-copy.yaml");
                copyFile(dir, outDirAbsPath, "count-latency.txt");
                copyFile(dir, outDirAbsPath, "restart-cost.txt");
                copyFile(dir, outDirAbsPath, "checkpoints.txt");

                analyzeLatency(dir, latencyResult);

                FileWriter fileWriter = new FileWriter(new File(outDirAbsPath , "throughputs.txt"));
                fileWriter.write("start || end || duration || num-elements || elements/second/core || MB/sec/core || GB received\n");
                for (int i = argIdx; i < args.length; i++) {
                    analyzeThroughput(dir, args[i], throughputResult, fileWriter);
                }
                fileWriter.close();

                writeLatencyThroughputStat(latencyResult, throughputResult, outDirAbsPath, "latency_throughput.txt");
                break;
            case "jm":
                // jm outputDir logFileName
                fileName = args[argIdx++];
                parseRestartCost(fileName,  dir, "restart-cost.txt");
                parseCheckpoint(fileName,  dir, "checkpoints.txt");
                break;
            case "tm":
                // tm outputDir ...logFilePaths
                FileWriter fw = new FileWriter(new File(dir ,"throughputs.txt"));
                fw.write("start || end || duration || num-elements || elements/second/core || MB/sec/core || GB received\n");
                for (int i = argIdx; i < args.length; i++) {
                    fileName = args[argIdx++];
                    gatherThroughputData(fileName, fw);
                }
                fw.close();
                break;
            default:
                break;
        }

    }

}

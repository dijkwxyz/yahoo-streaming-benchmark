package flink.benchmark.utils;

import flink.benchmark.BenchmarkConfig;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.java.tuple.Tuple4;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

//    public static void parseRestartCost(String srcFileName, String path, String dstFileName) throws IOException, ParseException {
//        Scanner sc = new Scanner(new File(srcFileName));
//
//        FileWriter fw = new FileWriter(new File(path, dstFileName));
//        String timePattern = ".*(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3} \\w{3})";
//        //2021-05-05 04:52:22,719 UTC INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: Kafka -> (Flat Map, Flat Map -> Filter -> Projection -> Flat Map -> Timestamps/Watermarks -> Map) (8/8) (9530062f70342991c61311d3d5f47eba) switched from RUNNING to FAILED on org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot@2b36afee.
//        Pattern failedPattern = Pattern.compile(timePattern + " .*switched from \\w+ to FAILED.*");
//        //2021-05-07 02:02:32,878 UTC INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Job WordCount Global Window Experiment (c405f119755983293e2309850970b3a0) switched from state RUNNING to RESTARTING.
//        Pattern restartPattern = Pattern.compile(timePattern + " .*to RESTARTING.*");
//        //2021-05-05 01:14:24,424 UTC INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Restoring job effa6bd99425bf0381a8a567c71e016b from latest valid checkpoint: Checkpoint 81 @ 1620177217703 for effa6bd99425bf0381a8a567c71e016b.
//        //2021-05-05 01:14:27,708 UTC INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Restoring job effa6bd99425bf0381a8a567c71e016b from latest valid checkpoint: Checkpoint 88 @ 1620177217703 for effa6bd99425bf0381a8a567c71e016b.
//        Pattern loadCheckpointPattern = Pattern.compile(timePattern + " .*Restoring job \\w+ from latest valid checkpoint: Checkpoint (\\d+) @ \\d+ for \\w+.*");
//        //2021-05-07 10:46:36,209 UTC INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - No checkpoint found during restore.
//        Pattern noCheckpointPattern = Pattern.compile(timePattern + " .*No checkpoint found during restore.*");
//
//        //2021-05-05 01:14:27,830 UTC INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Window(SlidingEventTimeWindows(10000, 2000), EventAndProcessingTimeTrigger, ProcessWindowFunction$1) -> Sink: Unnamed (8/8) (276a4225d3c078389e1a21c7b0e4c8e8) switched from DEPLOYING to RUNNING.
//        Pattern toRunningPattern = Pattern.compile(timePattern + " INFO.*switched from DEPLOYING to RUNNING.*");
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zzz");
//        //(timestamp, type, info, matched string)
//        ArrayList<Tuple4<Date, Signal, String, String>> arr = new ArrayList<>();
//        //define type constants
//        final int toFailed = 0;
//        final int toRestart = 1;
//        final int loadCheckpoint = 2;
//        final int toRunning = 3;
//        final int noCheckpoint = 4;
//
//        DescriptiveStatistics failedDS = new DescriptiveStatistics();
//        ArrayList<Date> failedArr = new ArrayList<>();
//        while (sc.hasNextLine()) {
//            String l = sc.nextLine();
//            Matcher failedMatcher = failedPattern.matcher(l);
//            Matcher restartMatcher = restartPattern.matcher(l);
//            Matcher loadCheckpointMatcher = loadCheckpointPattern.matcher(l);
//            Matcher noCheckpointMatcher = noCheckpointPattern.matcher(l);
//            Matcher toRunningMatcher = toRunningPattern.matcher(l);
//            if (failedMatcher.matches()) {
//                Date date = dateFormat.parse(failedMatcher.group(1));
//                arr.add(new Tuple4<>(date, toFailed, null, failedMatcher.group()));
//                failedArr.add(date);
//            } else if (restartMatcher.matches()) {
//                Date date = dateFormat.parse(restartMatcher.group(1));
//                arr.add(new Tuple4<>(date, toRestart, null, restartMatcher.group()));
//            } else if (loadCheckpointMatcher.matches()) {
//                Date date = dateFormat.parse(loadCheckpointMatcher.group(1));
//                arr.add(new Tuple4<>(date, loadCheckpoint, loadCheckpointMatcher.group(2), loadCheckpointMatcher.group()));
//            }else if (noCheckpointMatcher.matches()) {
//                Date date = dateFormat.parse(noCheckpointMatcher.group(1));
//                arr.add(new Tuple4<>(date, noCheckpoint, null, noCheckpointMatcher.group()));
//            } else if (toRunningMatcher.matches()) {
//                Date date = dateFormat.parse(toRunningMatcher.group(1));
//                arr.add(new Tuple4<>(date, toRunning, null, toRunningMatcher.group()));
//            }
//        }
//
//        for (int f = 1; f < failedArr.size(); f++) {
//            long timeDiff = failedArr.get(f).getTime() - failedArr.get(f - 1).getTime();
//            failedDS.addValue(timeDiff);
//        }
//
//
//        fw.write(String.format("MTTI: %f, total failures: %d\n", failedDS.getMean(), 1 + failedDS.getN()));
//
//        int i = 0;
//        //skip normal start
//        while (i < arr.size() && arr.get(i).f1 == toRunning) {
//            i++;
//        }
//
//        fw.write("checkpointId failedToRestart restartToLoadCP loadCPToRunning");
//        fw.write(' ');
//        fw.write("failedTime restartTime loadCheckpointTime toRunningTime");
//        fw.write('\n');
//
//        while (i < arr.size()) {
//            Date failedTime;
//            Date restartTime;
//            Date toRunningTime;
//            Date loadCheckpointTime;
//            String checkpointId;
//
//            //skip state before first failure
//            while(i < arr.size() && arr.get(i).f1 != toFailed) {
//                i++;
//            }
//            if (i >= arr.size()) {
//                break;
//            }
//
//            failedTime = arr.get(i).f0;
//            i++;
//            if (i >= arr.size()) {
//                break;
//            }
//
//            assert (arr.get(i).f1 == toRestart);
//            restartTime = arr.get(i).f0;
//            i++;
//            if (i >= arr.size()) {
//                break;
//            }
//
//            assert (arr.get(i).f1 == loadCheckpoint || arr.get(i).f1 == noCheckpoint);
//            loadCheckpointTime = arr.get(i).f0;
//            checkpointId = arr.get(i).f1 == loadCheckpoint ? arr.get(i).f2 : "-1";
//            i++;
//            if (i >= arr.size()) {
//                break;
//            }
//
//            //take the last toRunning record
//            Tuple4<Date, Signal, String, String> prevRecord = arr.get(i);
//            i++;
//            while (i < arr.size() && arr.get(i).f1 == toRunning) {
//                prevRecord = arr.get(i);
//                i++;
//            }
//            toRunningTime = prevRecord.f0;
//
//
//            fw.write(checkpointId);
//
//            fw.write(' ');
//            fw.write(String.valueOf(restartTime.getTime() - failedTime.getTime()));
//            fw.write(' ');
//            fw.write(String.valueOf(loadCheckpointTime.getTime() - restartTime.getTime()));
//            fw.write(' ');
//            fw.write(String.valueOf(toRunningTime.getTime() - loadCheckpointTime.getTime()));
//
//            fw.write(' ');
//            fw.write(String.valueOf(failedTime.getTime()));
//            fw.write(' ');
//            fw.write(String.valueOf(restartTime.getTime()));
//            fw.write(' ');
//            fw.write(String.valueOf(loadCheckpointTime.getTime()));
//            fw.write(' ');
//            fw.write(String.valueOf(toRunningTime.getTime()));
//
//            fw.write('\n');
//
//        }
//
//        sc.close();
//        fw.close();
//    }

    private static String timePattern = ".*(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3} \\w{3})";
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS zzz");

    //define type constants
    private enum Signal {
        taskFailed,
        restoreFromCheckpoint,
        noCheckpoint,
        taskCancelled,
        loadCheckpointComplete
    }

    //(timestamp, type, info, matched string)
    public static DescriptiveStatistics parseJMForFailureTime(
            String srcAbsPath,
            ArrayList<Tuple4<Date, Signal, String, String>> JmSignals) throws IOException, ParseException {
        Scanner sc = new Scanner(new File(srcAbsPath));

        //2021-09-08 00:40:00,034 UTC INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Job AdvertisingTopologyFlinkWindows (b9579b1becd96d98a25fe2f42cb88d73) switched from state RUNNING to RESTARTING.
        Pattern failedPattern = Pattern.compile(timePattern + " .*switched from state RUNNING to RESTARTING.*");
        //2021-05-05 01:14:24,424 UTC INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Restoring job effa6bd99425bf0381a8a567c71e016b from latest valid checkpoint: Checkpoint 81 @ 1620177217703 for effa6bd99425bf0381a8a567c71e016b.
        //2021-05-05 01:14:27,708 UTC INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Restoring job effa6bd99425bf0381a8a567c71e016b from latest valid checkpoint: Checkpoint 88 @ 1620177217703 for effa6bd99425bf0381a8a567c71e016b.
        Pattern restoreCheckpointPattern = Pattern.compile(timePattern + " .*Restoring job \\w+ from latest valid checkpoint: Checkpoint (\\d+) @ \\d+ for \\w+.*");
        //2021-05-07 10:46:36,209 UTC INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - No checkpoint found during restore.
        Pattern noCheckpointPattern = Pattern.compile(timePattern + " .*No checkpoint found during restore.*");

        DescriptiveStatistics failedDS = new DescriptiveStatistics();
        while (sc.hasNextLine()) {
            String l = sc.nextLine();
            Matcher failedMatcher = failedPattern.matcher(l);
            Matcher restoreCheckpointMatcher = restoreCheckpointPattern.matcher(l);
            Matcher noCheckpointMatcher = noCheckpointPattern.matcher(l);
            if (failedMatcher.matches()) {
                Date date = dateFormat.parse(failedMatcher.group(1));
                JmSignals.add(new Tuple4<>(date, Signal.taskFailed, null, failedMatcher.group()));
            } else if (restoreCheckpointMatcher.matches()) {
                Date date = dateFormat.parse(restoreCheckpointMatcher.group(1));
                JmSignals.add(new Tuple4<>(date, Signal.restoreFromCheckpoint, restoreCheckpointMatcher.group(2), restoreCheckpointMatcher.group()));
            } else if (noCheckpointMatcher.matches()) {
                Date date = dateFormat.parse(noCheckpointMatcher.group(1));
                JmSignals.add(new Tuple4<>(date, Signal.noCheckpoint, null, noCheckpointMatcher.group()));
            }
        }

        for (int f = 1; f < JmSignals.size(); f++) {
            if (JmSignals.get(f).f1 == Signal.taskFailed) {
                long timeDiff = JmSignals.get(f).f0.getTime() - JmSignals.get(f - 1).f0.getTime();
                failedDS.addValue(timeDiff);
            }
        }

        sc.close();
        return failedDS;
    }

    //(timestamp, type, info, matched string)
    public static void parseTmLogForRecoveryTime(
            String srcAbsPath,
            ArrayList<Tuple4<Date, Signal, String, String>> taskCancelledSignals,
            ArrayList<Tuple4<Date, Signal, String, String>> loadCheckpointCompleteSignals) throws IOException, ParseException {
        Scanner sc = new Scanner(new File(srcAbsPath));

        String timePattern = ".*(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3} \\w{3})";
        //2021-09-07 07:48:14,269 UTC INFO  org.apache.flink.runtime.taskexecutor.TaskExecutor           [] - Un-registering task and sending final execution state CANCELED to JobManager for task Source: Kafka -> (Process, Flat Map -> Filter -> Map -> Projection -> Flat Map -> Timestamps/Watermarks -> Map) (6/8) d2e54e8977271191a9b4c7f5f4e6829c.
        Pattern taskCancelledPattern = Pattern.compile(timePattern + " .*Un-registering task and sending final execution state CANCELED to JobManager for task.*");
        //2021-09-07 07:49:25,678 UTC INFO  org.apache.flink.runtime.state.heap.HeapKeyedStateBackend    [] - Initializing heap keyed state backend with stream factory.
        Pattern loadCheckpointCompletePattern = Pattern.compile(timePattern + " .*Initializing heap keyed state backend with stream factory.*");

        while (sc.hasNextLine()) {
            String l = sc.nextLine();
            Matcher taskCancelledMatcher = taskCancelledPattern.matcher(l);
            Matcher loadCheckpointCompleteMatcher = loadCheckpointCompletePattern.matcher(l);
            if (taskCancelledMatcher.matches()) {
                Date date = dateFormat.parse(taskCancelledMatcher.group(1));
                taskCancelledSignals.add(new Tuple4<>(date, Signal.taskCancelled, null, taskCancelledMatcher.group()));
            } else if (loadCheckpointCompleteMatcher.matches()) {
                Date date = dateFormat.parse(loadCheckpointCompleteMatcher.group(1));
                loadCheckpointCompleteSignals.add(new Tuple4<>(date, Signal.loadCheckpointComplete, null, loadCheckpointCompleteMatcher.group()));
            }
        }

        sc.close();
    }
//
//    private static void deduplicateWithinTimeDiff(
//            ArrayList<Tuple4<Date, Signal, String, String>> arr, int threshold_ms) {
//
//        ArrayList<Tuple4<Date, Signal, String, String>> newArr = new ArrayList<>();
//
//        for (int i = 1; i < arr.size(); i++) {
//            long prevTime = arr.get(i - 1).f0.getTime();
//            long currTime = arr.get(i).f0.getTime();
//            // if time diff is less than 1 sec,
//            // it belongs to different parallel task of the same failure,
//            // so do not keep the signal
//            if (currTime - prevTime < threshold_ms) {
//                continue;
//            }
//            //else keep the last signal
//            else {
//                newArr.add(arr.get(i - 1));
//            }
//        }
//        //keep the last one
//        newArr.add(arr.get(arr.size() - 1));
//
//        arr.clear();
//        arr.addAll(newArr);
//    }

    //(timestamp, type, info, matched string)
    public static void parseRestartCost(
            String srcDir,
            String JmLog,
            List<String> tmLogs,
            String dstDir,
            String dstFileName) throws IOException, ParseException {

        //(timestamp, type, info, matched string)
        ArrayList<Tuple4<Date, Signal, String, String>> jmSignals = new ArrayList<>();
        ArrayList<Tuple4<Date, Signal, String, String>> taskCancelledSignals = new ArrayList<>();
        ArrayList<Tuple4<Date, Signal, String, String>> loadCheckpointCompleteSignals = new ArrayList<>();

        for (String tmLog : tmLogs) {
            String srcAbsName = new File(srcDir, tmLog).getAbsolutePath();
            parseTmLogForRecoveryTime(srcAbsName, taskCancelledSignals, loadCheckpointCompleteSignals);
        }

        //sort by timestamp
        taskCancelledSignals.sort(Comparator.comparing(a -> a.f0));
        loadCheckpointCompleteSignals.sort(Comparator.comparing(a -> a.f0));

        //consider the same signals within 3 secs as deduplicates
//        deduplicateWithinTimeDiff(taskCancelledSignals, 3000);

        DescriptiveStatistics failedDS = parseJMForFailureTime(
                new File(srcDir, JmLog).getAbsolutePath(), jmSignals);

        //write results
        FileWriter fw = new FileWriter(new File(dstDir, dstFileName));
        fw.write(String.format("MTTI: %f, total failures: %d\n", failedDS.getMean(), 1 + failedDS.getN()));
        fw.write("checkpointId failedTime RecoveryStartTime loadCheckpointCompleteTime\n");

        int jmSignalsIdx = 0;
        int taskCancelledIdx = 0;
        int loadCheckpointIdx = 0;
        while (jmSignalsIdx < jmSignals.size()) {
            //starts with a taskFailed signal
            Tuple4<Date, Signal, String, String> signal = jmSignals.get(jmSignalsIdx++);
            if (signal.f1 != Signal.taskFailed) {
                continue;
            }
            Date failedTime = signal.f0;
//
//            //skip initializing backend signal on first start-up
//            while (loadCheckpointIdx < loadCheckpointCompleteSignals.size()) {
//                Tuple4<Date, Signal, String, String> currLoadCheckpointSignal =
//                        loadCheckpointCompleteSignals.get(loadCheckpointIdx);
//                if (currLoadCheckpointSignal.f0.compareTo(failedTime) > 0) {
//                    break;
//                }
//                loadCheckpointIdx++;
//            }

            //if no checkpoint to restore, set checkpoint id as -1
            Tuple4<Date, Signal, String, String> restoreFrom = jmSignals.get(jmSignalsIdx++);
            assert restoreFrom.f1 == Signal.noCheckpoint || restoreFrom.f1 == Signal.restoreFromCheckpoint;
            String checkpointId = restoreFrom.f1 == Signal.noCheckpoint ? "-1" : restoreFrom.f2;

            if (taskCancelledIdx >= taskCancelledSignals.size()) {
                break;
            }

            Tuple4<Date, Signal, String, String> nextFailedSignal = null;
            if (jmSignalsIdx < jmSignals.size()) {
                nextFailedSignal = jmSignals.get(jmSignalsIdx);
                assert nextFailedSignal.f1 == Signal.taskFailed;
            }
            FindLastBeforeSignalParam findLastTaskCancelledParam =
                    new FindLastBeforeSignalParam(taskCancelledIdx, taskCancelledSignals, nextFailedSignal);
            Tuple4<Date, Signal, String, String> lastTaskCancelledBeforeNextFailed =
                    findLastBeforeSignal(findLastTaskCancelledParam, -1);
            //take out index
            taskCancelledIdx = findLastTaskCancelledParam.toFindIndex;
            String recoveryStartTimeStr = lastTaskCancelledBeforeNextFailed == null
                    ? "-1":
                    String.valueOf(lastTaskCancelledBeforeNextFailed.f0.getTime());
//            Tuple4<Date, Signal, String, String> taskCancelledSignal = taskCancelledSignals.get(taskCancelledIdx++);
//            Date recoveryStartTime = taskCancelledSignal.f0;

            if (loadCheckpointIdx >= loadCheckpointCompleteSignals.size()) {
                break;
            }
            Tuple4<Date, Signal, String, String> nextTaskCancelledSignal = null;
            if (taskCancelledIdx < taskCancelledSignals.size()) {
                nextTaskCancelledSignal = taskCancelledSignals.get(taskCancelledIdx);
            }
            FindLastBeforeSignalParam findLastLoadCheckpointParam =
                    new FindLastBeforeSignalParam(loadCheckpointIdx, loadCheckpointCompleteSignals, nextTaskCancelledSignal);
            Tuple4<Date, Signal, String, String> lastLoadCheckpointBeforeNextTaskCancelled =
                    findLastBeforeSignal(findLastLoadCheckpointParam, 8);
            //take out index
            loadCheckpointIdx = findLastLoadCheckpointParam.toFindIndex;
            String recoveryEndTimeStr = lastLoadCheckpointBeforeNextTaskCancelled == null
                    ? "-1"
                    : String.valueOf(lastLoadCheckpointBeforeNextTaskCancelled.f0.getTime());

            fw.write(checkpointId);
            fw.write(' ');
            fw.write(String.valueOf(failedTime.getTime()));
            fw.write(' ');
            fw.write(recoveryStartTimeStr);
            fw.write(' ');
            fw.write(recoveryEndTimeStr);
            fw.write('\n');
        }

        fw.close();
    }

    private static class FindLastBeforeSignalParam {
        int toFindIndex;
        List<Tuple4<Date, Signal, String, String>> toFindSignals;
        Tuple4<Date, Signal, String, String> nextSignal;

        public FindLastBeforeSignalParam(int toFindIndex, List<Tuple4<Date, Signal, String, String>> toFindSignals, Tuple4<Date, Signal, String, String> nextSignal) {
            this.toFindIndex = toFindIndex;
            this.toFindSignals = toFindSignals;
            this.nextSignal = nextSignal;
        }
    }
    
    private static Tuple4<Date, Signal, String, String> findLastBeforeSignal(
            FindLastBeforeSignalParam param,
            int numSubTask
    ) {
        Tuple4<Date, Signal, String, String> toFindSignal = null;
        int ct = 0;
        if (param.nextSignal == null) {
            toFindSignal = param.toFindSignals.get(param.toFindSignals.size() - 1);
        }
        //otherwise, use the last toFindSignal that is smaller than nextSignal
        else {
            while (param.toFindIndex < param.toFindSignals.size()) {
                Tuple4<Date, Signal, String, String> currToFindSignal =
                        param.toFindSignals.get(param.toFindIndex);
                if (currToFindSignal.f0.compareTo(param.nextSignal.f0) > 0) {
                    break;
                }

                toFindSignal = currToFindSignal;
                ct++;
                param.toFindIndex++;
            }

        }

        //not requring each subtask to generate a signal
        if (numSubTask <= 0) {
            return toFindSignal;
        }
        //each slot should have a signal
        return ct == 0 || (ct % numSubTask == 0) ? toFindSignal : null;
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
            ThroughputResult throughputResult) throws IOException {
        Scanner sc = new Scanner(new File(inDir, fileName));
        // data format
        // start || end || duration || num-elements || elements/second/core || MB/sec/core || GB_received
        // 1621437064490,1621437069120,4630,1000000,215982.7213822894,49.43452180075594,0

        // throughput = elements/second/core
        DescriptiveStatistics throughputs = new DescriptiveStatistics();
        // skip header line
        sc.nextLine();
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
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
        sb.append(String.format("%.0f", eventTimeLatencies.getMean())).append(",");
        sb.append(String.format("%.0f", eventTimeLatencies.getPercentile(50))).append(",");
        sb.append(String.format("%.0f", eventTimeLatencies.getPercentile(90))).append(",");
        sb.append(String.format("%.0f", eventTimeLatencies.getPercentile(95))).append(",");
        sb.append(String.format("%.0f", eventTimeLatencies.getPercentile(99))).append(",");
        sb.append(String.format("%.0f", eventTimeLatencies.getMin())).append(",");
        sb.append(String.format("%.0f", eventTimeLatencies.getMax())).append(",");
        sb.append(String.format("%d", eventTimeLatencies.getN()));
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
            sb.append("Mean event-time latency:   ").append(String.format("%.0f", eventTime.getMean()));
//            sb.append("      , ");
//            sb.append("Mean processing-time latency:   ").append(String.format("%.0f",procTime.getMean()));
            sb.append('\n');
            sb.append("Median event-time latency: ").append(String.format("%.0f", eventTime.getPercentile(50)));
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
        sb.append(String.format("%.0f", throughputs.getMean())).append(",");
        sb.append(String.format("%.0f", throughputs.getMax())).append(",");
        sb.append(String.format("%d", throughputs.getN()));
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
            sb.append("Mean throughput: ").append(String.format("%.0f", entry.getValue().getMean()));
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

    public static void copyFiles(String srcDir, String dstDir) throws IOException {
        Files.list(new File(srcDir).toPath()).forEach(
                path -> {
                    if (path.toFile().isFile()) {
                        try {
                            copyFile(srcDir, dstDir, path.getFileName().toString());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
        );
    }

    public static void main(String[] args) throws IOException, ParseException {
        // jm outputDir logFileName
        // args = "jm C:\\Users\\46522\\Downloads\\results\\ C:\\Users\\46522\\Downloads\\results\\flink-ec2-user-standalonesession-0-multilevel-benchmark-5.novalocal.log".split(" ");
        // tm outputDir ...logFilePaths
        // args = "tm C:\\Users\\46522\\Downloads\\results\\ C:\\Users\\46522\\Downloads\\results\\flink2.log C:\\Users\\46522\\Downloads\\results\\flink3.log".split(" ");
        // zk resultDir ...tmFileNames
        // args = "zk C:\\Users\\joinp\\Downloads\\results flink2 flink3 redis2".split(" ");
        int argIdx = 0;
        String mode = args[argIdx++];
        String srcDir = args[argIdx++];
        String fileName;
        switch (mode) {
            case "zk":
                // zk resultDir ...tmFileNames
                BenchmarkConfig config = new BenchmarkConfig(new File(srcDir, "conf-copy.yaml").getAbsolutePath());
                String load = String.valueOf(config.loadTargetHz);
                String backend = config.multilevelEnable ? "multi" : "single";
                System.out.println("load = " + load);
                String date = new SimpleDateFormat("MM-dd_HH-mm-ss").format(new Date());
                String generatedPrefix = String.format("%s_load-%s-%s/", date, load, backend);
                File generatedDir = new File(srcDir, generatedPrefix);
                if (!generatedDir.exists()) {
                    generatedDir.mkdir();
                }
                String outDirAbsPath = generatedDir.getAbsolutePath();
                copyFiles(srcDir, outDirAbsPath);


                //get tm hosts
                List<String> tmHosts = new ArrayList<>();
                for (int i = argIdx; i < args.length; i++) {
                    tmHosts.add(args[i]);
                }

                LatencyResult latencyResult = new LatencyResult();
                ThroughputResult throughputResult = new ThroughputResult();
                for (String tmHost : tmHosts) {
                    analyzeThroughput(srcDir, tmHost + ".txt", throughputResult);
                }

                List<String> tmLogs = tmHosts.stream().map(s -> s + ".log").collect(Collectors.toList());
                parseRestartCost(srcDir, "flink1.log", tmLogs, outDirAbsPath, "restart-cost.txt");

                analyzeLatency(srcDir, latencyResult);

                writeLatencyThroughputStat(latencyResult, throughputResult, outDirAbsPath, "latency_throughput.txt");
                break;
            case "jm":
                // jm outputDir logFileName
                fileName = args[argIdx++];
                parseCheckpoint(fileName, srcDir, "checkpoints.txt");
                break;
            case "tm":
                // tm outputDir ...logFilePaths
                FileWriter fw = new FileWriter(new File(srcDir, "throughputs.txt"));
                fw.write("start,end,duration,numElements,elements/second/core,MB/sec/core,GbReceived\n");
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

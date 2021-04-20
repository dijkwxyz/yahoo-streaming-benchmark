package flink.benchmark.utils;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyzeTool {

    public static class Result {

        DescriptiveStatistics latencies = new DescriptiveStatistics();;
        SummaryStatistics throughputs = new SummaryStatistics();;
        Map<String, DescriptiveStatistics> perHostLat = new HashMap<>();
        Map<String, SummaryStatistics> perHostThr = new HashMap<>();

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

    public static Result analyzeLatency(String path, String host, Result result) throws FileNotFoundException {
        Scanner sc = new Scanner(new File(path, host+"-data"));

        String l;

        DescriptiveStatistics latencies = new DescriptiveStatistics();
        while (sc.hasNextLine()) {
            l = sc.nextLine();
            int latency = Integer.parseInt(l);
            latencies.addValue(latency);
            result.latencies.addValue(latency);
        }
        result.perHostLat.put(host, latencies);

        return result;
    }

    public static void main(String[] args) throws IOException {
        /*
         path prefix
         hostname
         */
        args = new String[]{
                "C:\\Users\\joinp\\Downloads\\",
                "flink2"
//                "flink3"
        };
        String prefix = args[0];
        Result result = new Result();
        for (int i = 1; i < args.length; i++) {
            analyzeThroughput(prefix, args[i], result);
            analyzeLatency(prefix, args[i], result);
        }

        SummaryStatistics throughputs = result.throughputs;
        DescriptiveStatistics latencies = result.latencies;

        System.out.println("====== " + "all-machines" + " =======");

        System.out.println("lat-mean;lat-median;lat-90percentile;lat-95percentile;lat-99percentile;throughput-mean;throughput-max;latencies;throughputs;");
        System.out.println(latencies.getMean() + ";" + latencies.getPercentile(50) + ";" + latencies.getPercentile(90) + ";" + latencies.getPercentile(95) + ";" + latencies.getPercentile(99) + ";" + throughputs.getMean() + ";" + throughputs.getMax() + ";" + latencies.getN() + ";" + throughputs.getN());

        for (Map.Entry<String, DescriptiveStatistics> entry : result.perHostLat.entrySet()) {
            System.err.println("====== " + entry.getKey() + " (entries: " + entry.getValue().getN() + ") =======");
            System.err.println("Mean latency " + entry.getValue().getMean());
            System.err.println("Median latency " + entry.getValue().getPercentile(50));
        }

        System.err.println("================= Throughput (in total " + result.perHostThr.size() + " reports ) =====================");
        for (Map.Entry<String, SummaryStatistics> entry : result.perHostThr.entrySet()) {
            System.err.println("====== " + entry.getKey() + " (entries: " + entry.getValue().getN() + ")=======");
            System.err.println("Mean throughput " + entry.getValue().getMean());
        }

    }


}


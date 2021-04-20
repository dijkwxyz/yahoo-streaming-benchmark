package flink.benchmark.utils;

import flink.benchmark.BenchmarkConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;

public class StateBackendFactory {
    private static String MemoryStateBackendName = "memory";
    private static String FsStateBackendName = "fs";
    public static StateBackend create(String name, String uri, BenchmarkConfig config) {
        if (MemoryStateBackendName.equals(name)) {
            return new MemoryStateBackend(config.maxMemStateSize, true);
        }
        else if (FsStateBackendName.equals(name)) {
            return new FsStateBackend(uri, true);
        }
        else {
            return null;
        }
    }
}

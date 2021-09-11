package flink.benchmark.utils;

import flink.benchmark.BenchmarkConfig;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;

import java.io.IOException;

public class StateBackendFactory {
    private static String MemoryStateBackendName = "memory";
    private static String FsStateBackendName = "fs";
    private static String RocksDbStateBackendName = "rocksDB";
    public static StateBackend create(String name, String uri, BenchmarkConfig config) throws IOException {
        if (MemoryStateBackendName.equals(name)) {
            return new MemoryStateBackend(config.maxMemStateSize, true);
        }
        else if (FsStateBackendName.equals(name)) {
            return new FsStateBackend(uri, true);
        }
        else if (RocksDbStateBackendName.equals(name)){
            return new RocksDBStateBackend(uri, false);
        } else {
            throw new IllegalArgumentException("Invalid backend name.");
        }
    }
}

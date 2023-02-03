package org.corfudb.benchmark;

import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.FlushOptions;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.Statistics;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SnapshotCreationBenchmark extends UniverseHook {
    static {
        RocksDB.loadLibrary();
    }

    private static final int memtableSize = 1024*1024;
    private static final int payloadSize = 1024;
    private static final int versionCount = 128;
    private static final int txSize = 10;
    private static final int minThreadCount = 1;
    private static final int maxThreadCount = 4;

    @Param({"true", "false"})
    private boolean useSnapshots;

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        OptimisticTransactionDB rocksDb;
        org.rocksdb.Options options = new org.rocksdb.Options()
                /*
                 * Amount of data to build up in memtables across all column families before writing
                 * to disk. This is distinct from ColumnFamilyOptions.writeBufferSize(), which
                 * enforces a limit for a single memtable. This feature is disabled by default.
                 * Specify a non-zero value to enable it. Default: 0 (disabled)
                 *
                 * Each new key-value pair is first written to the memtable. Memtable size is controlled
                 * by the option write_buffer_size. It's usually not a big memory consumer. However,
                 * memtable size is inversely proportional to write amplification -- the more memory you
                 * give to the memtable, the less the write amplification is. If you increase your
                 * memtable size, be sure to also increase your L1 size! L1 size is controlled by
                 * the option max_bytes_for_level_base.
                 */
                .setDbWriteBufferSize(memtableSize)
                .setStatistics(new Statistics())
                .setTableFormatConfig(new BlockBasedTableConfig().setNoBlockCache(true))
                .setUseDirectReads(true)
                .setCreateIfMissing(true);
    }

    public static void main(String[] args) throws RunnerException {
        for (int threadCount = minThreadCount; threadCount <= maxThreadCount; threadCount = threadCount * 2) {
            Options opt = jmhBuilder()
                    .threads(threadCount)
                    .build();

            Collection<RunResult> results = new Runner(opt).run();
        }
    }

    private final WriteOptions writeOptions = new WriteOptions()
            .setDisableWAL(true)
            .setSync(false);

    private final FlushOptions flushOptions = new FlushOptions()
            .setWaitForFlush(true)
            .setAllowWriteStall(true);

    private byte[] intArray(int number) {
        return ByteBuffer.allocate(4).putInt(number).array();
    }

    @Setup(Level.Iteration)
    public void prime(BenchmarkState state) throws IOException, RocksDBException {
        Path dbPath = Files.createTempDirectory("rocksDb");
        state.rocksDb = OptimisticTransactionDB.open(state.options, dbPath.toFile().getAbsolutePath());

        for (int i = 0; i < versionCount; i++) {
            for (int subset = 0; subset < txSize; subset++) {
                final String payload = RandomStringUtils.random(payloadSize);
                state.rocksDb.put(intArray(txSize * i + subset), payload.getBytes());
            }
        }

        state.rocksDb.syncWal();
        state.rocksDb.flush(flushOptions);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration(BenchmarkState state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException, RocksDBException {
        state.rocksDb.closeE();
    }

    @TearDown
    public void tearDown(BenchmarkState state) {
    }


    @Benchmark
    @Measurement(iterations = 1)
    @Warmup(iterations = 1)
    public void simplePut(BenchmarkState state) throws RocksDBException {
        final Map<Integer, Snapshot> snapshotMap = new HashMap<>();

        for (int i = 0; i < versionCount; i++) {
            for (int subset = 0; subset < txSize; subset++) {
                final String payload = RandomStringUtils.random(payloadSize);
                state.rocksDb.put(intArray(txSize * i + subset), payload.getBytes());
            }

            if (useSnapshots) {
                snapshotMap.put(i, state.rocksDb.getSnapshot());
            }
        }

        snapshotMap.forEach((version, snapshot) -> state.rocksDb.releaseSnapshot(snapshot));
    }

}

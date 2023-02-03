package org.corfudb.benchmark;

import lombok.extern.slf4j.Slf4j;
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
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.FlushOptions;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RocksBenchmark extends UniverseHook {
    static {
        RocksDB.loadLibrary();
    }
    private static final int memtableSize = 1024*1024;
    private static final int payloadSize = 1024;
    private static final int versionCount = 128;
    private static final int txSize = 10;

    @Param({"true", "false"})
    private boolean useSnapshots;

    private static final int minThreadCount = 4;
    private static final int maxThreadCount = 64;

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        OptimisticTransactionDB rocksDb;
        Map<Integer, Snapshot> snapshotMap = new HashMap<>();
        ConcurrentLinkedQueue<Integer> threadSnapshots = new ConcurrentLinkedQueue<>();

        // Out of box, RocksDB will use LRU-based block cache implementation with 8MB capacity.

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
    public void prime(BenchmarkState state, BenchmarkParams params) throws IOException, RocksDBException {
        log.info("Creating DB.");
        Path dbPath = Files.createTempDirectory("rocksDb");
        state.options.statistics().setStatsLevel(StatsLevel.ALL);
        state.rocksDb = OptimisticTransactionDB.open(state.options, dbPath.toFile().getAbsolutePath());

        log.debug("Started priming...");
        for (int i = 0; i < versionCount; i++) {
            for (int subset = 0; subset < txSize; subset++) {
                final String payload = RandomStringUtils.random(payloadSize);
                state.rocksDb.put(intArray(txSize * i + subset), payload.getBytes());
            }
        }

        state.rocksDb.syncWal();
        state.rocksDb.flush(flushOptions);
        log.debug("... done.");

        log.debug("Creating snapshots...");

        for (int i = 0; i < versionCount; i++) {
            for (int subset = 0; subset < txSize; subset++) {
                final String payload = RandomStringUtils.random(payloadSize);
                state.rocksDb.put(intArray(txSize * i + subset), payload.getBytes());
            }

            if (useSnapshots) {
                state.snapshotMap.put(i, state.rocksDb.getSnapshot());
            }
        }
        log.info("... done.");
    }
    @Setup(Level.Invocation)
    public void setup(BenchmarkState state, BenchmarkParams params) throws IOException, RocksDBException {
        int increment = versionCount/params.getThreads();
        for (int count = 0; count < versionCount; count += increment) {
            state.threadSnapshots.add(count);
        }
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration(BenchmarkState state) throws RocksDBException {
        log.trace("RocksDB Stats: {}", state.options.statistics());
        state.snapshotMap.forEach((version, snapshot) -> state.rocksDb.releaseSnapshot(snapshot));
        state.rocksDb.closeE();
    }

    @Benchmark
    @Warmup(iterations = 1)
    @Measurement(iterations = 1)
    public void simplePut(BenchmarkState state, Blackhole blackhole) throws RocksDBException {
        log.trace("Spawning a thread...");
        int version = state.threadSnapshots.poll();
        log.trace("Using snapshot version {}.", version);
        ReadOptions readOptions = new ReadOptions().setSnapshot(state.snapshotMap.get(version));
        for (int i = 0; i < versionCount; i++) {
            for (int subset = 0; subset < txSize; subset++) {
                byte[] key = intArray(txSize * i + subset);
                if (useSnapshots) {
                    blackhole.consume(state.rocksDb.get(readOptions, key));
                } else {
                    blackhole.consume(state.rocksDb.get(key));
                }
            }
        }
    }
}

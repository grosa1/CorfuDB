package org.corfudb.benchmark;

import org.apache.commons.lang3.RandomStringUtils;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
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

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TxBenchmark extends UniverseHook {

    @Param({"true", "false"})
    boolean diskBacked;

    private final int payloadSize = 10;
    private final int maxTxCount = 256;
    private final int maxPutCount = 64;

    private static final int minThreadCount = 1;
    private static final int maxThreadCount = 32;

    @State(Scope.Benchmark)
    public static class BenchmarkState extends UniverseBenchmarkState {
        Table<Schema.Uuid, Schema.StringValue, ?> table;
        CorfuStore store;
        int iterationCount = 0;
    }


    public static void main(String[] args) throws RunnerException {
        for (int threadCount = minThreadCount; threadCount <= maxThreadCount; threadCount = threadCount * 2) {
            Options opt = jmhBuilder()
                    .threads(threadCount)
                    .build();

            Collection<RunResult> results = new Runner(opt).run();
        }
    }

    @Setup
    public void setup(BenchmarkState state) {
        setupUniverseFramework(state);
    }

    @Setup(Level.Iteration)
    public void prepare(BenchmarkState state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        state.store = new CorfuStore(state.corfuCluster.getLocalCorfuClient().getRuntime());
        state.table = openTable(state);
    }

    private Table<Schema.Uuid, Schema.StringValue, ?> openTable(
            BenchmarkState state) throws
            InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        TableOptions.TableOptionsBuilder optionsBuilder = TableOptions.builder();

        if (diskBacked) {
            final String diskBackedDirectory = "/tmp/";
            final Path persistedCacheLocation = Paths.get(diskBackedDirectory,
                    DEFAULT_STREAM_NAME + state.iterationCount++);
            optionsBuilder.persistentDataPath(persistedCacheLocation);
        }

        return state.store.openTable(
                DEFAULT_STREAM_NAMESPACE,
                DEFAULT_STREAM_NAME,
                Schema.Uuid.class,
                Schema.StringValue.class,
                null,
                optionsBuilder.build()
                );
    }

    @TearDown
    public void tearDown(BenchmarkState state) {
        state.corfuClient.shutdown();
        state.wf.getUniverse().shutdown();
    }


    @Benchmark
    @Measurement(iterations = 1)
    @Warmup(iterations = 1)
    public void singleThreadedWrite(BenchmarkState state) {
        Random random = new Random();
        for (int txCount = 0; txCount < maxTxCount; txCount++) {
            TxnContext tx = state.store.txn(DEFAULT_STREAM_NAMESPACE);

            for (int putCount = 0; putCount < maxPutCount; putCount++) {
                Schema.Uuid key = Schema.Uuid.newBuilder().setLsb(random.nextLong())
                        .setMsb(random.nextLong()).build();
                final String payload = RandomStringUtils.random(payloadSize);
                Schema.StringValue value = Schema.StringValue.newBuilder()
                        .setValue(payload)
                        .setSecondary(payload)
                        .build();
                tx.getRecord(state.table, key);
                tx.putRecord(state.table, key, value, null);
            }

            tx.commit();
        }
    }

}

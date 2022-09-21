package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSubscriber;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.utils.CommonTypes;
import org.corfudb.utils.LogReplicationStreams;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public abstract class DefaultAdapterForUpgrade implements ILogReplicationConfigAdapter {
    public static final String STREAMS_TEST_TABLE = "StreamsToReplicateTestTable";
    public static final String VERSION_TEST_TABLE = "VersionTestTable";

    public static final int MAP_COUNT = 5;
    public static final String SEPARATOR = "$";
    public static final String TABLE_PREFIX = "Table00";
    public static final String NAMESPACE = "LR-Test";
    public static final String TAG_ONE = "tag_one";
    public static final String SAMPLE_CLIENT = "SampleClient";

    final int indexOne = 1;
    final int indexTwo = 2;
    final Map<ReplicationSubscriber, Set<String>> streamsToReplicateMap = new HashMap<>();
    String versionString = "version_latest";
    CorfuStore corfuStore;

    protected void init() {
        Set<String> streams = new HashSet<>();
        if (corfuStore != null) {
            try {
                corfuStore.openTable(NAMESPACE, STREAMS_TEST_TABLE, LogReplicationStreams.TableInfo.class,
                    LogReplicationStreams.Namespace.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            } catch (Exception e) {
                // Just for wrap this up
            }

            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                Set<LogReplicationStreams.TableInfo> tables = txn.keySet(STREAMS_TEST_TABLE);
                tables.forEach(table -> streams.add(table.getName()));
                txn.commit();
            }
        } else {
            for (int i = 1; i <= MAP_COUNT; i++) {
                streams.add(NAMESPACE + SEPARATOR + TABLE_PREFIX + i);
            }
        }
        streamsToReplicateMap.put(new ReplicationSubscriber(
                LogReplicationMetadata.ReplicationModels.REPLICATE_FULL_TABLES, SAMPLE_CLIENT), streams);
    }

    @Override
    public String getVersion() {
        if (corfuStore != null) {
            try {
                corfuStore.openTable(NAMESPACE, VERSION_TEST_TABLE,
                        LogReplicationStreams.VersionString.class,
                        LogReplicationStreams.Version.class, CommonTypes.Uuid.class,
                        TableOptions.builder().build());
            } catch (Exception e) {
                // Just for wrap this up
            }

            LogReplicationStreams.VersionString versionStringKey =
                    LogReplicationStreams.VersionString.newBuilder()
                            .setName("VERSION").build();
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                versionString =
                        ((LogReplicationStreams.Version)txn.getRecord(VERSION_TEST_TABLE,
                                versionStringKey).getPayload()).getVersion();
                txn.commit();
            } catch (Exception e) {
                // Just for wrap this up
            }
        }
        return versionString;
    }

    @Override
    public Map<UUID, List<UUID>> getStreamingConfigOnSink() {
        Map<UUID, List<UUID>> streamsToTagsMaps = new HashMap<>();
        UUID streamTagOneDefaultId = TableRegistry.getStreamIdForStreamTag(NAMESPACE, TAG_ONE);
        streamsToTagsMaps.put(CorfuRuntime.getStreamID(NAMESPACE + SEPARATOR + TABLE_PREFIX + indexOne),
                Collections.singletonList(streamTagOneDefaultId));
        streamsToTagsMaps.put(CorfuRuntime.getStreamID(NAMESPACE + SEPARATOR + TABLE_PREFIX + indexTwo),
                Collections.singletonList(streamTagOneDefaultId));
        return streamsToTagsMaps;
    }

    @Override
    public Map<ReplicationSubscriber, Set<String>> getSubscriberToStreamsMap() {
        return streamsToReplicateMap;
    }
}

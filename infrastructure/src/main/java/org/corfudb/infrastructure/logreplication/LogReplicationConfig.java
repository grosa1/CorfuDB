package org.corfudb.infrastructure.logreplication;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * This class represents any Log Replication Configuration,
 * i.e., set of parameters common across all Clusters.
 */
@Slf4j
@Data
@ToString
public class LogReplicationConfig {

    // Log Replication message timeout time in milliseconds
    public static final int DEFAULT_TIMEOUT_MS = 5000;

    // Log Replication default max number of messages generated at the source cluster for each batch
    public static final int DEFAULT_MAX_NUM_MSG_PER_BATCH = 10;

    // Log Replication default max data message size is 64MB
    public static final int MAX_DATA_MSG_SIZE_SUPPORTED = (64 << 20);

    // Log Replication default max cache number of entries
    // Note: if we want to improve performance for large scale this value should be tuned as it
    // used in snapshot sync to quickly access shadow stream entries, written locally.
    // This value is exposed as a configuration parameter for LR.
    public static final int MAX_CACHE_NUM_ENTRIES = 200;

    // Percentage of log data per log replication message
    public static final int DATA_FRACTION_PER_MSG = 90;

    public static final UUID REGISTRY_TABLE_ID = CorfuRuntime.getStreamID(
        getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME));

    public static final UUID PROTOBUF_TABLE_ID = CorfuRuntime.getStreamID(
            getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME));

    // Set of streams that shouldn't be cleared on snapshot apply phase, as these streams should be the result of
    // "merging" the replicated data (from source) + local data (on sink).
    // For instance, RegistryTable (to avoid losing local opened tables on sink)
    public static final Set<UUID> MERGE_ONLY_STREAMS = new HashSet<>(Arrays.asList(
            REGISTRY_TABLE_ID,
            PROTOBUF_TABLE_ID
    ));

    private Set<String> streamsToReplicate;

    // Mapping from stream ids to their fully qualified names.
    private Map<UUID, String> streamIdsToNameMap;

    // Streaming tags on Sink (map data stream id to list of tags associated to it)
    private Map<UUID, List<UUID>> dataStreamToTagsMap;

    // Set of streams to drop on Sink if replication subscriber info differs from Source when both are on different
    // versions
    private Set<UUID> streamsToDrop;

    // Snapshot Sync Batch Size(number of messages)
    private int maxNumMsgPerBatch;

    // Max Size of Log Replication Data Message
    private int maxMsgSize;

    // Max Cache number of entries
    private int maxCacheSize;

    /**
     * The max size of data payload for the log replication message.
     */
    private int maxDataSizePerMsg;

    public static final String SAMPLE_CLIENT = "Sample Client";


    public LogReplicationConfig(Set<String> streamsToReplicate, Set<UUID> streamsToDrop,
                                Map<UUID, List<UUID>> streamToTagsMap, ServerContext serverContext, CorfuRuntime runtime) {
        this.streamsToReplicate = streamsToReplicate;
        this.streamsToDrop = streamsToDrop;
        this.dataStreamToTagsMap = streamToTagsMap;

        if (serverContext == null) {
            this.maxNumMsgPerBatch = DEFAULT_MAX_NUM_MSG_PER_BATCH;
            this.maxMsgSize = MAX_DATA_MSG_SIZE_SUPPORTED;
            this.maxCacheSize = MAX_CACHE_NUM_ENTRIES;
        } else {
            this.maxNumMsgPerBatch = serverContext.getLogReplicationMaxNumMsgPerBatch();
            this.maxMsgSize = serverContext.getLogReplicationMaxDataMessageSize();
            this.maxCacheSize = serverContext.getLogReplicationCacheMaxSize();
        }
        this.maxDataSizePerMsg = maxMsgSize * DATA_FRACTION_PER_MSG / 100;
    }
}

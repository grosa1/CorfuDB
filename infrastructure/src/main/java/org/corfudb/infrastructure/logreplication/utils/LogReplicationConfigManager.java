package org.corfudb.infrastructure.logreplication.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.config.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.config.LogReplicationFullTableConfig;
import org.corfudb.infrastructure.logreplication.config.LogReplicationLogicalGroupConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.LogReplication.ClientDestinationInfoKey;
import org.corfudb.runtime.LogReplication.ClientRegistrationId;
import org.corfudb.runtime.LogReplication.ClientRegistrationInfo;
import org.corfudb.runtime.LogReplication.DestinationInfoVal;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.LogReplication.ReplicationSubscriber;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.MERGE_ONLY_STREAMS;
import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.REGISTRY_TABLE_ID;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.LR_MODEL_METADATA_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.LR_REGISTRATION_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Handle construction and maintenance of the streams to replicate for all replication models supported in LR.
 * @author pankti-m
 */
@Slf4j
public class LogReplicationConfigManager {

    @Getter
    private final CorfuRuntime runtime;

    private final CorfuStore corfuStore;

    private final String localClusterId;

    private long lastRegistryTableLogTail = Address.NON_ADDRESS;

    @Getter
    private ServerContext serverContext;

    @Getter
    private final Map<LogReplicationSession, LogReplicationConfig> sessionToConfigMap = new ConcurrentHashMap<>();

    @Getter
    private final Set<ReplicationSubscriber> registeredSubscribers = new CopyOnWriteArraySet<>();

    // Map from a logical group to all the Sinks it is targeting. Note that we maintain two bidirectional maps
    // to facilitate the detection of Sink removal case.
    private final Map<String, Set<String>> groupSinksMap = new ConcurrentHashMap<>();

    // In-memory list of registry table entries for generating configs
    private List<Map.Entry<TableName, CorfuRecord<TableDescriptors, TableMetadata>>> registryTableEntries =
            new ArrayList<>();



    /**
     * Used for non-upgrade testing purpose only. Note that this constructor will keep the version table in
     * uninitialized state, in which case LR will be constantly considered to be not upgraded.
     */
    @VisibleForTesting
    public LogReplicationConfigManager(CorfuRuntime runtime, String localClusterId) {
        this.runtime = runtime;
        this.corfuStore = new CorfuStore(runtime);
        this.localClusterId = localClusterId;
        init();
    }

    public LogReplicationConfigManager(CorfuRuntime runtime, ServerContext serverContext, String localClusterId) {
        this.runtime = runtime;
        this.corfuStore = new CorfuStore(runtime);
        this.serverContext = serverContext;
        this.localClusterId = localClusterId;
        init();
    }

    /**
     * Init config manager:
     * 1. Adding default subscriber to registeredSubscribers
     * 2. Open client configuration tables to make them available in corfuStore
     * 3. Initialize registry table log tail and in-memory entries
     */
    private void init() {
        registeredSubscribers.add(SessionManager.getDefaultSubscriber());
        // TODO (V2): This builder should be removed after the rpc stream is added for Sink side session creation.
        //  and logical group subscribers should come from client registration.
        registeredSubscribers.add(SessionManager.getDefaultLogicalGroupSubscriber());
        openClientConfigTables();
        syncWithRegistryTable();
    }

    private void openClientConfigTables() {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                            LR_REGISTRATION_TABLE_NAME,
                            ClientRegistrationId.class,
                            ClientRegistrationInfo.class,
                            null,
                            TableOptions.fromProtoSchema(ClientRegistrationInfo.class));
                    corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                            LR_MODEL_METADATA_TABLE_NAME,
                            ClientDestinationInfoKey.class,
                            DestinationInfoVal.class,
                            null,
                            TableOptions.fromProtoSchema(ClientRegistrationInfo.class));
                } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                    log.error("Cannot open client config tables, retrying.", e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Failed to open client config tables", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    public void generateConfig(Set<LogReplicationSession> sessions) {
        sessions.forEach(session -> {
                switch (session.getSubscriber().getModel()) {
                    case FULL_TABLE:
                        log.debug("Generating FULL_TABLE config for session {}",
                                TextFormat.shortDebugString(session));
                        generateFullTableConfig(session);
                        break;
                    case LOGICAL_GROUPS:
                        log.debug("Generating LOGICAL_GROUP config for session {}",
                                TextFormat.shortDebugString(session));
                        generateLogicalGroupConfig(session);
                        break;
                    default: break;
                }
        });
    }

    private void generateFullTableConfig(LogReplicationSession session) {
        Map<UUID, List<UUID>> streamToTagsMap = new HashMap<>();
        Set<UUID> streamsToDrop = new HashSet<>();
        Set<String> streamsToReplicate = new HashSet<>();

        registryTableEntries.forEach(entry -> {
            String tableName = TableRegistry.getFullyQualifiedTableName(entry.getKey());
            boolean isFederated = entry.getValue().getMetadata().getTableOptions().getIsFederated();
            UUID streamId = CorfuRuntime.getStreamID(tableName);

            // Find federated tables that will be used by FULL_TABLE replication model
            if (isFederated || MERGE_ONLY_STREAMS.contains(streamId)) {
                streamsToReplicate.add(tableName);
                // Collect tags for this stream
                List<UUID> tags = streamToTagsMap.getOrDefault(streamId, new ArrayList<>());
                tags.addAll(entry.getValue().getMetadata().getTableOptions().getStreamTagList().stream()
                        .map(streamTag -> TableRegistry.getStreamIdForStreamTag(entry.getKey().getNamespace(), streamTag))
                        .collect(Collectors.toList()));
                streamToTagsMap.put(streamId, tags);
            } else {
                streamsToDrop.add(streamId);
            }
        });

        if (sessionToConfigMap.containsKey(session)) {
            LogReplicationFullTableConfig config = (LogReplicationFullTableConfig) sessionToConfigMap.get(session);
            config.setStreamsToReplicate(streamsToReplicate);
            config.setDataStreamToTagsMap(streamToTagsMap);
            config.setStreamsToDrop(streamsToDrop);
        } else {

            LogReplicationFullTableConfig fullTableConfig = new LogReplicationFullTableConfig(session, streamsToReplicate,
                    streamToTagsMap, serverContext, streamsToDrop);
            sessionToConfigMap.put(session, fullTableConfig);

        }

        log.info("LogReplicationFullTableConfig generated for session={}, streams to replicate={}, streamsToDrop={}",
                TextFormat.shortDebugString(session), streamsToReplicate, streamsToDrop);
    }

    private void generateLogicalGroupConfig(LogReplicationSession session) {
        Map<UUID, List<UUID>> streamToTagsMap = new HashMap<>();
        Set<String> streamsToReplicate = new HashSet<>();
        // Check if the local cluster is the Sink for this session. Sink side will honor whatever Source side send
        // instead of relying on groupSinksMap to filter the streams to replicate.
        boolean isSink = session.getSinkClusterId().equals(localClusterId);
        Map<String, Set<String>> logicalGroupToStreams = new HashMap<>();
        if (!isSink) {
            groupSinksMap.entrySet().stream()
                    .filter(entry -> entry.getValue().contains(session.getSinkClusterId()))
                    .map(Map.Entry::getKey)
                    .forEachOrdered(group -> {
                        logicalGroupToStreams.put(group, new HashSet<>());
                    });
            log.debug("Targeted logical groups={}", logicalGroupToStreams.keySet());
        }

        registryTableEntries.forEach(entry -> {
            String tableName = TableRegistry.getFullyQualifiedTableName(entry.getKey());
            UUID streamId = CorfuRuntime.getStreamID(tableName);

            // Add merge only streams first
            if (MERGE_ONLY_STREAMS.contains(streamId)) {
                streamsToReplicate.add(tableName);
                // Collect tags for this stream
                List<UUID> tags = streamToTagsMap.getOrDefault(streamId, new ArrayList<>());
                tags.addAll(entry.getValue().getMetadata().getTableOptions().getStreamTagList().stream()
                        .map(streamTag -> TableRegistry.getStreamIdForStreamTag(entry.getKey().getNamespace(), streamTag))
                        .collect(Collectors.toList()));
                streamToTagsMap.put(streamId, tags);
            }

            // Find streams to replicate for every logical group for this session
            if (entry.getValue().getMetadata().getTableOptions().hasReplicationGroup()) {
                String clientName = entry.getValue().getMetadata().getTableOptions().getReplicationGroup().getClientName();
                String logicalGroup = entry.getValue().getMetadata().getTableOptions().getReplicationGroup().getLogicalGroup();
                // TODO (V2): Client name should be checked after the rpc stream is added for Sink side session creation.
//                if (session.getSubscriber().getClientName().equals(clientName) && groups.contains(logicalGroup)) {
                if (isSink || logicalGroupToStreams.containsKey(logicalGroup)) {
                    streamsToReplicate.add(tableName);
                    Set<String> relatedStreams = logicalGroupToStreams.getOrDefault(logicalGroup, new HashSet<>());
                    relatedStreams.add(tableName);
                    logicalGroupToStreams.put(logicalGroup, relatedStreams);
                    // Collect tags for this stream
                    List<UUID> tags = streamToTagsMap.getOrDefault(streamId, new ArrayList<>());
                    tags.addAll(entry.getValue().getMetadata().getTableOptions().getStreamTagList().stream()
                            .map(streamTag -> TableRegistry.getStreamIdForStreamTag(entry.getKey().getNamespace(), streamTag))
                            .collect(Collectors.toList()));
                    streamToTagsMap.put(streamId, tags);
                }
            }
        });

        if (sessionToConfigMap.containsKey(session)) {
            LogReplicationLogicalGroupConfig config = (LogReplicationLogicalGroupConfig) sessionToConfigMap.get(session);
            config.setStreamsToReplicate(streamsToReplicate);
            config.setDataStreamToTagsMap(streamToTagsMap);
            config.setLogicalGroupToStreams(logicalGroupToStreams);
        } else {
            LogReplicationLogicalGroupConfig logicalGroupConfig = new LogReplicationLogicalGroupConfig(session,
                    streamsToReplicate, streamToTagsMap, serverContext, logicalGroupToStreams);
            sessionToConfigMap.put(session, logicalGroupConfig);
        }

        log.info("LogReplicationLogicalGroupConfig generated for session={}, streams to replicate={}, groups={}",
                TextFormat.shortDebugString(session), streamsToReplicate, logicalGroupToStreams);
    }

    private void updateLogicalGroupConfig(LogReplicationSession session, Set<String> groupsToAdd, Set<String> groupsToRemove) {
        Preconditions.checkState(Collections.disjoint(groupsToAdd, groupsToRemove),
                "The sets groupsToAdd and groupsToRemove must not have any intersections");

        LogReplicationLogicalGroupConfig config = (LogReplicationLogicalGroupConfig) sessionToConfigMap.get(session);
        groupsToAdd.forEach(group -> {
            config.getRemovedGroupToStreams().remove(group);
        });
        registryTableEntries.forEach(entry -> {
            String tableName = TableRegistry.getFullyQualifiedTableName(entry.getKey());

            // Find streams to replicate for every logical group for this session
            if (entry.getValue().getMetadata().getTableOptions().hasReplicationGroup()) {
                String clientName = entry.getValue().getMetadata().getTableOptions().getReplicationGroup().getClientName();
                String logicalGroup = entry.getValue().getMetadata().getTableOptions().getReplicationGroup().getLogicalGroup();
                // TODO (V2): Client name should be checked after the rpc stream is added for Sink side session creation.
//                if (session.getSubscriber().getClientName().equals(clientName) && groups.contains(logicalGroup)) {
                if (groupsToRemove.contains(logicalGroup)) {
                    Set<String> relatedStreams = config.getRemovedGroupToStreams().getOrDefault(logicalGroup, new HashSet<>());
                    relatedStreams.add(tableName);
                    config.getRemovedGroupToStreams().put(logicalGroup, relatedStreams);
                }
            }
        });
    }

    public void getUpdatedConfig() {
        // Check if the registry table has new entries.  Otherwise, no update is necessary.
        if (syncWithRegistryTable()) {
            generateConfig(sessionToConfigMap.keySet());
        }
    }

    /**
     * Check if the registry table's log tail moved or not. If it has moved ahead, we need to update
     * lastRegistryTableLogTail and in-memory registry table entries, which will be used for construct
     * LogReplicationConfig objects.
     *
     * @return True if registry table has new entries and the in-memory data structures are updated.
     */
    private synchronized boolean syncWithRegistryTable() {
        // TODO (V2 / Chris): the operation of check current tail + generate updated config + update last log tail
        //  should be atomic operation in multi replication session env.
        StreamAddressSpace currentAddressSpace = runtime.getSequencerView().getStreamAddressSpace(
            new StreamAddressRange(REGISTRY_TABLE_ID, Long.MAX_VALUE, Address.NON_ADDRESS));
        long currentLogTail = currentAddressSpace.getTail();

        // Check if the log tail of registry table moved ahead
        if (currentLogTail != lastRegistryTableLogTail) {
            lastRegistryTableLogTail = currentLogTail;
            registryTableEntries = runtime.getTableRegistry().getRegistryTable().entryStream().collect(Collectors.toList());
            return true;
        }

        return false;
    }

    /**
     * Preprocess the registration and logical group configuration tables, get the log tail that will be the start
     * point for client config listener to monitor updates.
     *
     * @return Log tail hat will be the start point for client config listener to monitor updates.
     */
    public CorfuStoreMetadata.Timestamp preprocessAndGetTail() {
        CorfuStoreMetadata.Timestamp logTail;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            List<CorfuStoreEntry<ClientRegistrationId, ClientRegistrationInfo, Message>> registrationResults =
                    txn.executeQuery(LR_REGISTRATION_TABLE_NAME, record -> true);
            registrationResults.forEach(entry -> {
                String clientName = entry.getKey().getClientName();
                ReplicationModel model = entry.getPayload().getModel();
                ReplicationSubscriber subscriber = ReplicationSubscriber.newBuilder()
                        .setClientName(clientName).setModel(model).build();
                // TODO (V2): currently we don't support ccustomized client name, default logical group subscriber
                //  should be removed after the grpc stream for Sink session creation is created.
                if (model.equals(ReplicationModel.LOGICAL_GROUPS)) {
                    subscriber = SessionManager.getDefaultLogicalGroupSubscriber();
                }
                registeredSubscribers.add(subscriber);
            });

            List<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, Message>> groupConfigResults =
                    txn.executeQuery(LR_MODEL_METADATA_TABLE_NAME, record -> true);
            groupConfigResults.forEach(entry -> {
                String clientName = entry.getKey().getClientName();
                ReplicationModel model = entry.getKey().getModel();
                ReplicationSubscriber subscriber = ReplicationSubscriber.newBuilder()
                        .setClientName(clientName).setModel(model).build();
                String groupName = entry.getKey().getGroupName();
                // TODO (V2): currently we don't support ccustomized client name, default logical group subscriber
                //  should be removed after the grpc stream for Sink session creation is created.
                if (model.equals(ReplicationModel.LOGICAL_GROUPS)) {
                    subscriber = SessionManager.getDefaultLogicalGroupSubscriber();
                }
                if (registeredSubscribers.contains(subscriber)) {
                    groupSinksMap.put(groupName, new HashSet<>(entry.getPayload().getDestinationIdsList()));
                } else {
                    log.warn("Subscriber {} not registered, but found its group config in client table: {}",
                            subscriber, groupName);
                }
            });

            logTail = txn.commit();
        }
        return logTail;
    }

    /**
     * Callback method which is invoked when the client config listener receives update for new client registration.
     */
    public void onNewClientRegister(ReplicationSubscriber subscriber) {
        if (registeredSubscribers.contains(subscriber)) {
            log.warn("Client {} with model {} already registered!", subscriber.getClientName(), subscriber.getModel());
            return;
        }
        // TODO (V2): Currently we add a default subscriber for logical group use case instead of listening on client
        //  registration. Subscriber should be added upon registration after grpc stream for session creation is added.
        registeredSubscribers.add(subscriber);
    }


    /**
     * Callback method which is invoked when the client config listener receives update for adding/removing
     * Sinks from a logical group.
     *
     * @param subscriber   Registered replication subscriber that changes the group destination mapping.
     * @param logicalGroup Logical group that is being modified.
     * @param destinations The updated list of destinations that this logicalGroup is targeting to.
     * @return A set of LogReplicationSession that needs a forced snapshot sync
     */
    public Set<LogReplicationSession> onGroupDestinationsChange(ReplicationSubscriber subscriber,
                                                                String logicalGroup,
                                                                List<String> destinations) {
        if (!registeredSubscribers.contains(subscriber)) {
            log.error("Client {} has not registered with replication model {}. Please register first.",
                    subscriber.getClientName(), subscriber.getModel());
            return null;
        }

        Set<String> recordedSinks = groupSinksMap.getOrDefault(logicalGroup, new HashSet<>());
        Set<String> updatedSinks = new HashSet<>(destinations);

        // Specifically for those who are newly added to this logicalGroup, we trigger a forced snapshot sync for
        // their ongoing replication sessions.
        Set<String> addedSinks = updatedSinks.stream().filter(sink -> !recordedSinks.contains(sink))
                .collect(Collectors.toSet());
        Set<String> removedSinks = recordedSinks.stream().filter(sink -> !updatedSinks.contains(sink))
                .collect(Collectors.toSet());

        // Update groupSinksMap first and then generate config, as config generation relies on groupSinksMap as well.
        groupSinksMap.put(logicalGroup, updatedSinks);


        Set<LogReplicationSession> sessionsAddedGroup = sessionToConfigMap.keySet().stream()
                .filter(session -> addedSinks.contains(session.getSinkClusterId()))
                .collect(Collectors.toSet());
        Set<LogReplicationSession> sessionsRemovedGroup = sessionToConfigMap.keySet().stream()
                .filter(session -> removedSinks.contains(session.getSinkClusterId()))
                .collect(Collectors.toSet());
        sessionsRemovedGroup.forEach(session -> {
            updateLogicalGroupConfig(session, new HashSet<>(), Collections.singleton(logicalGroup));
        });
        sessionsAddedGroup.forEach(session -> {
            updateLogicalGroupConfig(session, Collections.singleton(logicalGroup), new HashSet<>());
        });

        sessionsAddedGroup.addAll(sessionsRemovedGroup);
        generateConfig(sessionsAddedGroup);

        log.info("Updated group to sinks map, group={}, updatedSinks={}", logicalGroup, updatedSinks);

        return sessionsAddedGroup;
    }

    /**
     * Client listener will perform a full sync upon its resume. We perform a re-processing of the client config
     * tables in the same transaction (as the listener finds the timestamp from which to subscribe for deltas) to
     * avoid data loss.
     */
    public CorfuStoreMetadata.Timestamp onClientListenerResume() {
        registeredSubscribers.clear();
        groupSinksMap.clear();
        return preprocessAndGetTail();
    }

    public UUID getOpaqueStreamToTrack(ReplicationSubscriber subscriber) {
        switch(subscriber.getModel()) {
            case FULL_TABLE:
                return ObjectsView.getLogReplicatorStreamId();
            case LOGICAL_GROUPS:
                return ObjectsView.getLogicalGroupStreamTagInfo(subscriber.getClientName()).getStreamId();
            default:
                log.error("Unsupported replication model received: {}", subscriber.getModel());
                return null;
        }
    }
}

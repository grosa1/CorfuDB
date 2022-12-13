package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.NodeConfigurationMsg;

import java.util.Objects;
import java.util.UUID;

/**
 * This class represents a Log Replication Node
 */
@Slf4j
public class NodeDescriptor {

    @Getter
    private final String host;

    @Getter
    private final String port;

    @Getter
    private final String clusterId;

    @Getter
    private final String connectionId;    // Connection Identifier (APH UUID in the case of NSX, used by IClientChannelAdapter)

    @Getter
    private final String nodeId;      // Represents the node's identifier as tracked by the Topology Provider

    public NodeDescriptor(String host, String port, String siteId, String connectionId, String nodeId) {
        this.host = host;
        this.port = port;
        this.clusterId = siteId;
        this.connectionId = connectionId;
        this.nodeId = nodeId;
    }

    public NodeConfigurationMsg convertToMessage() {
        NodeConfigurationMsg nodeConfig = NodeConfigurationMsg.newBuilder()
                .setAddress(host)
                .setPort(Integer.parseInt(port))
                .setConnectionId(connectionId)
                .setNodeId(nodeId).build();
        return nodeConfig;
    }

    public String getEndpoint() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return String.format("Node: %s, %s", nodeId, getEndpoint());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeDescriptor that = (NodeDescriptor) o;
        return clusterId.equals(that.clusterId) && connectionId.equals(that.connectionId) && host.equals(that.host) &&
                nodeId.equals(that.nodeId) && port.equals(that.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId, connectionId, host, nodeId, port);
    }

}

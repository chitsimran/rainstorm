package com.example;

import com.example.enums.NodeState;

import java.io.Serializable;

/**
 * Represents a node entry in the membership list.
 * Contains nodeId, connection info
 */
public class Member implements Comparable<Member>, Serializable {
    public final String nodeId;
    public String host;
    public int port;
    public int dfsPort;
    public NodeState state;
    private long hash;

    /**
     * Constructs a Member object.
     *
     * @param nodeId        unique ID of node
     * @param host          host/IP address
     * @param port          port number
     * @param state         node state
     */
    public Member(String nodeId, String host, int port, NodeState state) {
        this.state = state;
        this.host = host;
        this.port = port;
        this.nodeId = nodeId;
        this.dfsPort = this.port + 1;
    }

    // used for dummy objects (for binary search)
    public Member(long hash) {
        this.nodeId = "DUMMY_OBJECT";
        this.hash = hash;
    }

    /**
     * @return host/IP of the node
     */
    public String getHost() {
        return this.host;
    }

    /**
     * get hash
     */
    public long getHash() {
        return hash;
    }

    /**
     * setter to recompute hash after rejoin)
     */
    public void recomputeHash() {
        this.hash = HashUtils.hashfileId(nodeId);
    }

    /**
     * @return port number of the node
     */
    public int getPort() {
        return this.port;
    }

    @Override
    public int compareTo(Member that) {
        return Long.compare(this.hash, that.hash);
    }
}

package com.example.enums;

/**
 * Represents the state of a node in the membership list.
 */
public enum NodeState {

    /** Node is healthy */
    ALIVE,

    /** Node is temporarily suspected of failure. */
    SUSPECTED,

    /** Node has been confirmed as failed after timeout. */
    FAILED,

    /** Node voluntarily left the group */
    LEFT
}

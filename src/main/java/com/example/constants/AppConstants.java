package com.example.constants;

public final class AppConstants {
    public static final int REPLICATION_FACTOR = 3;
    public static final int LINES_PER_SECOND = 100;
    public static final String LEADER_IP = "127.0.0.1";
    public static final int LEADER_CONTROL_PORT = 9100;
    public static final int DFS_PORT = 6662;
    public static final int BUFFER_SIZE = 400;
    public static final int WORKER_PORT = 9000;

    private AppConstants() {
    }
}

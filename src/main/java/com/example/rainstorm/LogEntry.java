package com.example.rainstorm;

import java.util.HashMap;
import java.util.Map;

public class LogEntry {
    public Type type;
    public String recordId;
    public Map<LogEntryField, String> fields = new HashMap<>();

    public LogEntry() {
    }

    public LogEntry(Type type, String recordId) {
        this.type = type;
        this.recordId = recordId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(type).append("|").append(recordId);
        for (var e : fields.entrySet()) {
            sb.append("|").append(e.getKey()).append("=").append(e.getValue());
        }
        return sb.toString();
    }

    public enum Type {
        RECEIVED, PROCESSED, OUTPUT, ACK_SENT, ACK_RECEIVED
    }
}

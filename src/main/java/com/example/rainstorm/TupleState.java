package com.example.rainstorm;

import java.util.ArrayList;
import java.util.List;

public class TupleState {
    String tupleId;
    boolean received;
    String sender; // task id of sender
    boolean processed;
    boolean output;
    boolean ackSent;
    List<TupleOutput> outputs = new ArrayList<>();
    String receivedKey;
    String processedKey;
    String receivedValue;
    String processedValue;
    boolean errorWhileProcessing;

    public TupleState(String tupleId) {
        this.tupleId = tupleId;
    }

    public static class TupleOutput {
        String id;
        String taskId;
        boolean ackReceived;

        public TupleOutput(String taskId) {
            this.taskId = taskId;
        }
    }
}

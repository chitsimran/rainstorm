package com.example.rainstorm;

public class LogEntryUtils {
    public static String logProcessedTuple(String tupleId, String outId, String key, String value) {
        return "PROCESSED|" + tupleId + "|" + "out_id=" + outId + "|" + "key=" + key + "|" + "value=" + value;
    }

    public static String logEmitTuple(String tupleId, String outId, String toTask) {
        return "OUTPUT|" + tupleId + "|to_task=" + toTask;
    }
}

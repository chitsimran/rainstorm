package com.example.rainstorm;

public enum LogEntryField {
    KEY("key"),
    VALUE("value"),
    SENDER("sender"),
    TO_TASK("to_task");

    private final String field;

    LogEntryField(String field) {
        this.field = field;
    }

    public String getField() {
        return this.field;
    }
}

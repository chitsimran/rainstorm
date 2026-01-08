package com.example.rainstorm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LogEntryReader {
    public static List<LogEntry> readAll(String filePath) throws IOException {
        List<LogEntry> entries = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                LogEntry entry = parseLine(line);
                if (entry != null) {
                    entries.add(entry);
                }
            }
        }
        return entries;
    }

    private static LogEntry parseLine(String line) {
        String[] parts = line.split("\\|");
        if (parts.length < 2) return null;

        LogEntry e = new LogEntry();
        e.type = LogEntry.Type.valueOf(parts[0]);
        e.recordId = parts[1];

        for (int i = 2; i < parts.length; i++) {
            String[] kv = parts[i].split("=", 2);
            if (kv.length == 2) {
                e.fields.put(LogEntryField.valueOf(kv[0]), kv[1]);
            }
        }
        return e;
    }
}

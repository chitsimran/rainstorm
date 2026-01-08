package com.example.op;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import rainstorm.Rainstorm;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AggregateByKey {
    public static void main(String[] args) throws IOException {
        int column = Integer.parseInt(args[0]);
        Map<String, Integer> map = new HashMap<>();
        while (true) {
            Rainstorm.OP op = Rainstorm.OP.parseDelimitedFrom(System.in);
            if (op.getKey().equals("EXIT")) break;
            CSVParser parser = CSVParser.parse(op.getValue(), CSVFormat.DEFAULT);
            CSVRecord record = parser.getRecords().get(0);
            String key;
            if (record.size() <= column)
                key = "";
            else key = record.get(column);
            if (key == null) key = "";
            map.merge(key, 1, Integer::sum);
            Rainstorm.OP opOutput = Rainstorm.OP.newBuilder().setKey(key).setValue(key + "=" + map.get(key)).build();
            opOutput.writeDelimitedTo(System.out);
            System.out.flush();
        }
    }
}

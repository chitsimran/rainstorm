package com.example.op;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import rainstorm.Rainstorm;

import java.io.IOException;

public class Transform {
    public static void main(String[] args) throws IOException {
        while (true) {
            Rainstorm.OP op = Rainstorm.OP.parseDelimitedFrom(System.in);
            if (op.getKey().equals("EXIT")) break;
            CSVParser parser = CSVParser.parse(op.getValue(), CSVFormat.DEFAULT);
            CSVRecord record = parser.getRecords().get(0);
            String c1 = record.size() > 0 ? record.get(0) : "";
            String c2 = record.size() > 1 ? record.get(1) : "";
            String c3 = record.size() > 2 ? record.get(2) : "";
            Rainstorm.OP output = Rainstorm.OP.newBuilder().setKey(op.getKey()).setValue(c1 + "," + c2 + "," + c3).build();
            output.writeDelimitedTo(System.out);
            System.out.flush();
        }
    }
}

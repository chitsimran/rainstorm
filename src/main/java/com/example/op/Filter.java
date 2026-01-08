package com.example.op;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import rainstorm.Rainstorm;

import java.io.IOException;
import java.util.Objects;

public class Filter {
    public static void main(String[] args) throws IOException {
        String filter = args[0];
        String addArgs = args[1];
        boolean isAggregate = !Objects.equals(addArgs, "NONE");
        int column = isAggregate ? Integer.parseInt(addArgs) : -1;
        while (true) {
            Rainstorm.OP op = Rainstorm.OP.parseDelimitedFrom(System.in);
            if (op.getKey().equals("EXIT")) break;
            Rainstorm.OP.Builder output = Rainstorm.OP.newBuilder();
            if (op.getValue().contains(filter)) {
                String key;
                if (isAggregate) {
                    CSVParser parser = CSVParser.parse(op.getValue(), CSVFormat.DEFAULT);
                    CSVRecord record = parser.getRecords().get(0);
                    if (record.size() <= column)
                        key = "";
                    else key = record.get(column);
                    if (key == null) key = "";
                } else {
                    key = op.getKey();
                }
                output.setKey(key).setValue(op.getValue());
            } else {
                //do nothing
            }
            output.build().writeDelimitedTo(System.out);
            System.out.flush();
        }
    }
}

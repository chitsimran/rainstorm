package com.example.rainstorm;

import com.google.common.util.concurrent.RateLimiter;
import rainstorm.Rainstorm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

import static com.example.constants.AppConstants.LINES_PER_SECOND;

public class SourceStreamTask extends Task {
    private final long inputRate;
    private String sourceFile;

    public SourceStreamTask(Rainstorm.Metadata metadata, Map<String, Rainstorm.Task> tasks, String taskId, String logFile, boolean autoScaleEnabled) {
        super(metadata, tasks, taskId, logFile);
        this.sourceFile = metadata.getMyTask().getSourceFile();
        if (autoScaleEnabled) {
            inputRate = metadata.getMyTask().getInputRate();
        } else {
            inputRate = LINES_PER_SECOND;
        }
    }

    @Override
    public void start() {
        logger.info("Starting source stream...");
        running = true;
        ackReceivedThread.start();
        logAndSendThread.start();
        outputToHydfsThread.start();
        startDataListener();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
        RateLimiter limiter = RateLimiter.create(inputRate);
        int lineNumber = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(sourceFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                limiter.acquire();
                lineNumber++;
                TupleState ts = new TupleState(UUID.randomUUID().toString());
                ts.processedKey = sourceFile + ":" + lineNumber;
                ts.processedValue = line;
                ts.processed = true;
                tuples.put(ts.tupleId, ts);
                emitTuple(ts.tupleId, sourceFile + ":" + lineNumber, line, true);
            }
            while (!pending.isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (Exception ignored) {
                }
                logger.info("Waiting for {} pending tuples", pending.size());
            }
            logger.info("Finished reading file");
        } catch (IOException e) {
            logger.error("Error reading file: {}", e.getMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Error running Source stream thread: {}", e.getMessage());
        }
    }

    @Override
    public void stop() {
        super.stop();
        sendStoppedMessage();
    }
}

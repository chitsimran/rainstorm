package com.example.rainstorm;

import com.example.MessageUtils;
import rainstorm.Rainstorm;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.example.MessageUtils.localReadRequest;
import static com.example.constants.AppConstants.BUFFER_SIZE;
import static com.example.constants.AppConstants.DFS_PORT;

public class WorkerTask extends Task {
    private final BlockingQueue<TupleState> receivedBuffer = new LinkedBlockingQueue<>();
    private final BlockingQueue<TupleState> processedBuffer = new LinkedBlockingQueue<>();
    private final BlockingQueue<TupleState> outputBuffer = new LinkedBlockingQueue<>();
    private final Thread receivedThread;
    private final Thread processedThread;
    private final Thread outputThread;
    private final ScheduledExecutorService loadScheduler = Executors.newSingleThreadScheduledExecutor();
    private Process process;
    private OutputStream stdin;
    private InputStream stdout;
    private String op_exe;
    private volatile long processedCount = 0;
    private boolean autoScaleEnabled;
    private int totalProcessed = 0;

    public WorkerTask(Rainstorm.Metadata metadata, Map<String, Rainstorm.Task> tasks, String taskId, String logFile, boolean autoScaleEnabled) {
        super(metadata, tasks, taskId, logFile);
        op_exe = metadata.getMyTask().getOpExe();
        this.autoScaleEnabled = autoScaleEnabled;
        String projectRoot = System.getProperty("user.dir");
        ProcessBuilder pb = new ProcessBuilder(
                "java", "-cp", projectRoot + "/target/rainstorm-1.0-SNAPSHOT.jar", "com.example.op." + op_exe, metadata.getMyTask().getOpArgs(), metadata.getMyTask().getAddOpArgs()
        );
        try {
            process = pb.start();
            stdin = process.getOutputStream();
            stdout = process.getInputStream();
        } catch (Exception e) {
            logger.error("Failed to run op_exe: {}, e: {}", op_exe, e.getMessage());
        }
        receivedThread = new Thread(this::flushReceivedBuffer, "worker-task-received-buffer");
        processedThread = new Thread(this::flushProcessedBuffer, "worker-task-processed-buffer");
        outputThread = new Thread(this::flushOutputBuffer, "worker-task-output-buffer");
    }

    public void recover() {
        try (Socket socket = new Socket(metadata.getMyTask().getIp(), DFS_PORT);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            Rainstorm.FileOpMessage message = localReadRequest(logFile, localLogFile);
            message.writeDelimitedTo(out);
            out.flush();
            Rainstorm.FileOpMessage ack = Rainstorm.FileOpMessage.parseDelimitedFrom(in);
            logger.info("Received ack response: {} for log recovery file: {}", ack.getType(), logFile);
        } catch (Exception e) {
            logger.error("Failed to get log file for recovery: {}", e.getMessage());
        }
        try {
            Map<String, TupleState> recoveredTuples = new HashMap<>();
            for (LogEntry e : LogEntryReader.readAll(localLogFile)) {
                //TODO extract this out so it adds tuplestate from log entry line
                TupleState st = recoveredTuples.computeIfAbsent(e.recordId, TupleState::new);
                switch (e.type) {
                    case RECEIVED:
                        st.received = true;
                        st.sender = e.fields.get(LogEntryField.SENDER);
                        st.receivedKey = e.fields.get(LogEntryField.KEY);
                        st.receivedValue = e.fields.get(LogEntryField.VALUE);
                        break;

                    case PROCESSED:
                        st.processed = true;
                        st.processedKey = e.fields.get(LogEntryField.KEY);
                        st.processedValue = e.fields.get(LogEntryField.VALUE);
                        break;

                    case OUTPUT:
                        st.output = true;
                        st.outputs.add(new TupleState.TupleOutput(e.fields.get(LogEntryField.TO_TASK)));
                        break;

                    case ACK_SENT:
                        st.ackSent = true;
                        break;

                    case ACK_RECEIVED:
                        if (!st.outputs.isEmpty()) {
                            Optional<TupleState.TupleOutput> output = st.outputs
                                    .stream()
                                    .filter(o -> o.taskId.equalsIgnoreCase(e.fields.get(LogEntryField.TO_TASK)))
                                    .findFirst();
                            output.ifPresent(tupleOutput -> tupleOutput.ackReceived = true);
                        }
                }
            }
            for (TupleState tuple : recoveredTuples.values()) {
                tuples.put(tuple.tupleId, tuple);
                if (tuple.outputs != null && !tuple.outputs.isEmpty() && tuple.outputs.get(0).ackReceived) {
                    continue;
                } else if (tuple.output) {
                    logger.info("[PENDING] Adding tuple: {} to pending", tuple.tupleId);
                    pending.putIfAbsent(tuple.tupleId, tuple);
                } else if (tuple.processed && !tuple.processedKey.isEmpty()) {
                    logger.info("[OUTPUT] Adding tuple: {} to output", tuple.tupleId);
                    outputBuffer.add(tuple);
                } else if (tuple.received && !tuple.processed) {
                    logger.info("[PROCESSED] Adding tuple: {} to processed", tuple.tupleId);
                    processedBuffer.add(tuple);
                }
            }
            logger.info("[RECOVERED] {} tuples", recoveredTuples.size());
        } catch (IOException e) {
            logger.error("Recovery failed for task: " + taskId + " e: " + e.getMessage());
        }
    }

    @Override
    protected void handleTupleMessage(Rainstorm.Message message) {
        if (tuples.containsKey(message.getTuple().getId())) {
            logger.info("DUPLICATE Received tuple with key: {} id: {}", message.getTuple().getKey(), message.getTuple().getId());
            sendAck(tuples.get(message.getTuple().getId()));
        } else {
            logger.info("Received tuple with key: {} id: {}", message.getTuple().getKey(), message.getTuple().getId());
            TupleState tupleState = new TupleState(message.getTuple().getId());
            tupleState.receivedKey = message.getTuple().getKey();
            tupleState.receivedValue = message.getTuple().getValue();
            tupleState.received = true;
            tupleState.sender = message.getTuple().getSenderTaskId();
            receivedBuffer.add(tupleState);
        }
    }

    @Override
    protected boolean isBufferEmpty() {
        return super.isBufferEmpty() && processedBuffer.isEmpty() && receivedBuffer.isEmpty() && outputBuffer.isEmpty();
    }

    private void flushOutputBuffer() {
        List<TupleState> batch = new ArrayList<>();
        long lastFlush = System.currentTimeMillis();
        final int BATCH_SIZE = BUFFER_SIZE;
        final int MAX_WAIT_MS = 500;
        try {
            while (running) {
                TupleState tp = outputBuffer.poll(10, TimeUnit.MILLISECONDS);
                if (tp != null) batch.add(tp);
                boolean batchFull = batch.size() >= BATCH_SIZE;
                boolean timeExceeded = System.currentTimeMillis() - lastFlush >= MAX_WAIT_MS;
                if ((batchFull || timeExceeded) && !batch.isEmpty()) {
                    processOutputBatch(batch);
                    batch.clear();
                    lastFlush = System.currentTimeMillis();
                }
            }
        } catch (InterruptedException e) {
            logger.error("Error while polling buffer: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void processOutputBatch(List<TupleState> batch) {
        logger.info("[OUTPUT] Processing output batch");
        for (TupleState tupleState : batch) {
            emitTuple(tupleState.tupleId, tupleState.processedKey, tupleState.processedValue, false);
        }
    }

    private void flushReceivedBuffer() {
        List<TupleState> batch = new ArrayList<>();
        long lastFlush = System.currentTimeMillis();
        final int BATCH_SIZE = BUFFER_SIZE;
        final int MAX_WAIT_MS = 500;
        try {
            while (running) {
                TupleState tp = receivedBuffer.poll(10, TimeUnit.MILLISECONDS);
                if (tp != null) batch.add(tp);
                boolean batchFull = batch.size() >= BATCH_SIZE;
                boolean timeExceeded = System.currentTimeMillis() - lastFlush >= MAX_WAIT_MS;
                if ((batchFull || timeExceeded) && !batch.isEmpty()) {
                    processReceived(batch);
                    batch.clear();
                    lastFlush = System.currentTimeMillis();
                }
            }
        } catch (InterruptedException e) {
            logger.error("Error while polling buffer: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void flushProcessedBuffer() {
        List<TupleState> batch = new ArrayList<>();
        long lastFlush = System.currentTimeMillis();
        final int BATCH_SIZE = BUFFER_SIZE;
        final int MAX_WAIT_MS = 500;
        try {
            while (running) {
                TupleState tp = processedBuffer.poll(10, TimeUnit.MILLISECONDS);
                if (tp != null) batch.add(tp);
                boolean batchFull = batch.size() >= BATCH_SIZE;
                boolean timeExceeded = System.currentTimeMillis() - lastFlush >= MAX_WAIT_MS;
                if ((batchFull || timeExceeded) && !batch.isEmpty()) {
                    processTupleBatch(batch);
                    batch.clear();
                    lastFlush = System.currentTimeMillis();
                }
            }
        } catch (InterruptedException e) {
            logger.error("Error while polling buffer: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void processTupleBatch(List<TupleState> batch) {
        logger.info("[PROCESS] Processing process batch");
        StringBuilder append = new StringBuilder();
        for (TupleState tuple : batch) {
            //TODO process method should also update tuple with processed key-value
            try {
                processTuple(tuple);
                LogEntry logEntry = new LogEntry(LogEntry.Type.PROCESSED, tuple.tupleId);
                logEntry.fields.put(LogEntryField.KEY, tuple.processedKey);
                logEntry.fields.put(LogEntryField.VALUE, tuple.processedValue);
                append.append(logEntry).append("\n");
                tuple.errorWhileProcessing = false;
            } catch (Exception e) {
                tuple.errorWhileProcessing = true;
                processedBuffer.add(tuple);
                logger.error("Error while processing tuple: {}, e: {}", tuple.receivedKey, e.getMessage());
            }
        }
        try {
            logToDisk(append);
            for (TupleState tuple : batch) {
                if (tuple.errorWhileProcessing) continue;
                tuple.processed = true;
                if (tuple.processedKey != null && !tuple.processedValue.isEmpty())
                    outputBuffer.add(tuple);
                sendAck(tuple);
            }
        } catch (IOException e) {
            logger.error("Error logging to disk, re-adding all tuples for processedBuffer batch");
            processedBuffer.addAll(batch);
        }
    }

    private void processTuple(TupleState tuple) throws IOException {
        processedCount++;
        totalProcessed++;
        Rainstorm.OP input = Rainstorm.OP.newBuilder().setKey(tuple.receivedKey).setValue(tuple.receivedValue).build();
        input.writeDelimitedTo(stdin);
        stdin.flush();
        Rainstorm.OP output = Rainstorm.OP.parseDelimitedFrom(stdout);
        tuple.processedKey = output.getKey();
        tuple.processedValue = output.getValue();
        logger.info("Processed tuple with key: {}, processedKey: {}", tuple.receivedKey, tuple.processedKey);
        logger.info("TOTAL_TUPLES_PROCESSED: {}", totalProcessed);
    }

    private void processReceived(List<TupleState> batch) {
        logger.info("[RECEIVED] Processing received batch");
        StringBuilder append = new StringBuilder();
        for (TupleState tuple : batch) {
            if (tuples.containsKey(tuple.tupleId))
                continue;
            LogEntry logEntry = new LogEntry(LogEntry.Type.RECEIVED, tuple.tupleId);
            logEntry.fields.put(LogEntryField.SENDER, tuple.sender);
            logEntry.fields.put(LogEntryField.KEY, tuple.receivedKey);
            logEntry.fields.put(LogEntryField.VALUE, tuple.receivedValue);
            append.append(logEntry).append("\n");
        }
        try {
            logToDisk(append);
            for (TupleState tuple : batch) {
                if (tuples.containsKey(tuple.tupleId)) continue;
                processedBuffer.add(tuple);
                tuples.put(tuple.tupleId, tuple);
            }
        } catch (IOException e) {
            logger.error("Error logging to disk, re-adding all tuples for receivedBuffer batch");
            receivedBuffer.addAll(batch);
        }
    }

    @Override
    public void start() {
        if (!running) {
            running = true;
            receivedThread.start();
            processedThread.start();
            outputThread.start();
            ackReceivedThread.start();
            logAndSendThread.start();
            outputToHydfsThread.start();
            recover();
            startDataListener();
            if (autoScaleEnabled)
                startLoadReporter();
        }
    }

    @Override
    public void stop() {
        super.stop();
        int maxWaitSeconds = 30;
        int waited = 0;
        while (!isBufferEmpty() && waited < maxWaitSeconds) {
            try {
                Thread.sleep(500);
                waited++;
                if (waited % 4 == 0) {
                    logger.info("Still draining buffers... ({}s)", waited / 2);
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for buffers to drain");
                Thread.currentThread().interrupt();
                break;
            }
        }
        running = false;
        sendStoppedMessage();
        resendScheduler.shutdown();
        listenerPool.shutdown();
    }

    public void startLoadReporter() {
        loadScheduler.scheduleAtFixedRate(() -> {
            try {
                long count = processedCount;
                this.processedCount = 0;
                String myIp = metadata.getMyTask().getIp();
                Rainstorm.ControlMessage msg =
                        MessageUtils.buildLoadReportMessage(
                                this.stage,
                                this.taskIndex,
                                count,
                                myIp,
                                taskId
                        );
                NetworkUtils.sendProtoMessage(
                        leaderIp,
                        leaderPort,
                        msg
                );
            } catch (Exception e) {
                logger.error("[Task] Failed to send LoadReport: {}", e.getMessage());
            }
        }, stage * 4L, 1, TimeUnit.SECONDS);
    }
}
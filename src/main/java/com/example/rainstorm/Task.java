package com.example.rainstorm;

import com.example.MessageUtils;
import com.example.constants.AppConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rainstorm.Rainstorm;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.example.MessageUtils.buildEOFMessage;
import static com.example.MessageUtils.buildTupleAckMessage;
import static com.example.MessageUtils.localAppendRequest;
import static com.example.MessageUtils.tupleMessage;
import static com.example.constants.AppConstants.BUFFER_SIZE;
import static com.example.constants.AppConstants.DFS_PORT;
import static com.example.constants.AppConstants.WORKER_PORT;

public abstract class Task {
    protected final String leaderIp = AppConstants.LEADER_IP;
    protected final int leaderPort = AppConstants.LEADER_CONTROL_PORT;
    protected final Map<String, TupleState> tuples = new HashMap<>();
    protected final Logger logger = LoggerFactory.getLogger(Task.class);
    protected final Thread logAndSendThread;
    protected final Thread ackReceivedThread;
    protected final Thread outputToHydfsThread;
    protected final ScheduledExecutorService resendScheduler = Executors.newScheduledThreadPool(1);
    protected final ExecutorService listenerPool = Executors.newCachedThreadPool();
    private final BlockingQueue<TupleState> logAndSendBuffer = new LinkedBlockingQueue<>();
    private final BlockingQueue<TupleState> ackReceivedBuffer = new LinkedBlockingQueue<>();
    private final BlockingQueue<TupleState> outputToHydfsBuffer = new LinkedBlockingQueue<>();
    public Rainstorm.Metadata metadata;
    public int pid;
    protected String taskId;
    protected int stage;
    protected int taskIndex;
    protected String logFile;
    protected String localLogFile;
    protected List<Rainstorm.Task> nextTasks = new ArrayList<>();
    protected Map<String, Rainstorm.Task> tasks;
    protected int port;
    protected Map<String, TupleState> pending = new ConcurrentHashMap<>();
    protected volatile ServerSocket dataListenerSocket;
    boolean running = false;

    public Task(Rainstorm.Metadata metadata, Map<String, Rainstorm.Task> tasks, String taskId, String logFile) {
        this.metadata = metadata;
        this.tasks = tasks;
        this.taskId = taskId;
        this.logFile = logFile;
        this.localLogFile = "/home/chitsim2/local_" + logFile;
        this.port = metadata.getMyTask().getPort();
        this.stage = metadata.getMyTask().getStageValue();
        this.taskIndex = metadata.getMyTask().getTaskIndex();
        for (Rainstorm.Task task : metadata.getNextTasksList()) {
            nextTasks.add(task);
        }
        logAndSendThread = new Thread(this::flushLogAndSendBuffer, "task-send-and-log-buffer");
        ackReceivedThread = new Thread(this::flushAckReceivedBuffer, "task-ack-received-buffer");
        outputToHydfsThread = new Thread(this::flushOutputToHydfsBuffer, "output-hydfs-buffer");
        resendScheduler.scheduleWithFixedDelay(this::resendTuples, 7, 7, TimeUnit.SECONDS);
    }

    private void flushOutputToHydfsBuffer() {
        List<TupleState> batch = new ArrayList<>();
        long lastFlush = System.currentTimeMillis();
        final int BATCH_SIZE = BUFFER_SIZE;
        final int MAX_WAIT_MS = 500;
        try {
            while (running) {
                TupleState tp = outputToHydfsBuffer.poll(10, TimeUnit.MILLISECONDS);
                if (tp != null) batch.add(tp);
                boolean batchFull = batch.size() >= BATCH_SIZE;
                boolean timeExceeded = System.currentTimeMillis() - lastFlush >= MAX_WAIT_MS;
                if ((batchFull || timeExceeded) && !batch.isEmpty()) {
                    processOutputToHydfsBatch(batch);
                    batch.clear();
                    lastFlush = System.currentTimeMillis();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void processOutputToHydfsBatch(List<TupleState> batch) {
        StringBuilder append = new StringBuilder();
        for (TupleState tuple : batch) {
            append.append(tuple.processedValue).append("\n");
        }
        try {
            logToDisk(append, metadata.getMyTask().getDestinationFile());
            logger.info("Written to HyDFS file: {}", metadata.getMyTask().getDestinationFile());
        } catch (IOException e) {
            logger.error("Error logging to disk, re-adding all tuples for outputToHydfsBuffer batch");
            outputToHydfsBuffer.addAll(batch);
        }
    }

    protected void logToDisk(StringBuilder append, String dfsFile) throws IOException {
        byte[] data = append.toString().getBytes(StandardCharsets.UTF_8);
        Rainstorm.FileOpMessage message = localAppendRequest(data, dfsFile);
        try (Socket socket = new Socket(metadata.getMyTask().getIp(), DFS_PORT)) {
            logger.info("Sending append data of size: {} bytes to {}:{}", message.getFileData().toByteArray().length, metadata.getMyTask().getIp(), DFS_PORT);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            message.writeDelimitedTo(out);
            out.flush();
        } catch (Exception e) {
            throw e;
        }
    }

    protected void logToDisk(StringBuilder append) throws IOException {
        logToDisk(append, logFile);
    }

    protected void startDataListener() {
        listenerPool.submit(() -> {
            try {
                dataListenerSocket = new ServerSocket(port);
                running = true;
                while (!dataListenerSocket.isClosed()) {
                    try {
                        Socket socket = dataListenerSocket.accept();
                        listenerPool.submit(() -> handleConnection(socket));
                    } catch (SocketException e) {
                        logger.info("Data listener socket closed");
                        break;
                    }
                }
            } catch (Exception e) {
                logger.error("Data listener error: {}", e.getMessage());
            }
        });
    }

    private void handleConnection(Socket socket) {
        try (socket; InputStream in = socket.getInputStream()) {
            try {
                Rainstorm.Message msg = Rainstorm.Message.parseDelimitedFrom(in);
                if (msg != null) {
                    handleDataMessage(msg, socket);
                }
            } catch (Exception e) {
                logger.error("Error handling connection: {}", e.getMessage());
            }
        } catch (IOException ignored) {
        }
    }

    private void handleDataMessage(Rainstorm.Message msg, Socket socket) {
        switch (msg.getType()) {
            case PING:
                handleTaskPing(socket);
                break;
            case ACK:
                handleTaskAck(msg);
                break;
            case TUPLE:
                handleTupleMessage(msg);
                break;
            case TASK_METADATA:
                handleMetadataUpdate(msg);
                break;
            case STOP:
                stop();
                break;
            default:
                logger.error("[Task] Unknown message type: {}", msg.getType());
        }
    }

    private void handleMetadataUpdate(Rainstorm.Message message) {
        logger.info("Received UPDATE_TASK_METADATA");
        metadata = message.getMetadata();
        Map<String, Rainstorm.Task> newTasks = new HashMap<>();
        for (Rainstorm.Task task : message.getTasksList()) {
            newTasks.put(task.getId(), task);
        }
        tasks = newTasks;
        nextTasks = new ArrayList<>(metadata.getNextTasksList());
        logger.info("Updated TASK_METADATA");
    }

    protected void handleEOF(Socket socket) {
        Rainstorm.Message message = Rainstorm.Message.newBuilder()
                .setType(Rainstorm.Type.EOF_ACK)
                .build();
        try {
            message.writeDelimitedTo(socket.getOutputStream());
            socket.getOutputStream().flush();
        } catch (Exception ignored) {
        }
        new Thread(() -> {
            logger.info("EOF reached, waiting for all pending tuples to be acknowledged...");
            try {
                while (!isBufferEmpty()) {
                    Thread.sleep(1000);
                    logger.info("Still waiting for {} pending tuples", pending.size());
                }
                logger.info("All tuples acknowledged, stopping task");
                stop();
                running = false;
            } catch (InterruptedException e) {
                logger.error("EOF handler interrupted: {}", e.getMessage());
                Thread.currentThread().interrupt();
            }
        }, "eof-handler").start();
    }

    protected boolean isBufferEmpty() {
        return outputToHydfsBuffer.isEmpty() && pending.isEmpty() && logAndSendBuffer.isEmpty();
    }

    private void handleTaskAck(Rainstorm.Message message) {
        logger.info("Received ACK for tuple: {}", message.getTuple().getId());
        String tupleId = message.getTuple().getId();
        if (pending.containsKey(tupleId)) {
            TupleState tupleState = pending.get(tupleId);
            for (TupleState.TupleOutput output : tupleState.outputs) {
                output.ackReceived = true;
                ackReceivedBuffer.add(tupleState);
            }
            pending.remove(tupleId);
        }
    }

    private void handleTaskPing(Socket socket) {
        try {
            Rainstorm.ControlMessage pong = MessageUtils.buildTaskAckMessage(this.taskId);
            pong.writeDelimitedTo(socket.getOutputStream());
            socket.getOutputStream().flush();
        } catch (Exception e) {
            logger.error("Error sending PONG to task ping: {}", e.getMessage());
        }
    }

    protected void sendAck(TupleState tupleState) {
        Rainstorm.Task task = tasks.get(tupleState.sender);
        if (task == null) {
            logger.error("Didn't find Task for id: {}", tupleState.sender);
        } else {
            Rainstorm.Message message = buildTupleAckMessage(tupleState.tupleId);
            try (Socket socket = new Socket(task.getIp(), task.getPort())) {
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                message.writeDelimitedTo(out);
                out.flush();
                logger.info("Sent ACK for: {} to VM {}:{}", tupleState.tupleId, task.getIp(), task.getPort());
            } catch (Exception e) {
                logger.error("Error sending ACK for tuple: {} to taskID: {}, e: {}", tupleState.tupleId, task.getId(), e.getMessage());
            }
        }
    }

    /**
     * Sent when all tuples have been processed
     */
    protected void sendEOF() {
        Rainstorm.Message message = buildEOFMessage();
        int maxRetries = 6;
        long baseDelayMs = 100;
        long maxDelayMs = 3000;
        for (Rainstorm.Task task : nextTasks) {
            boolean acked = false;
            int attempt = 0;
            while (!acked && attempt < maxRetries) {
                try {
                    Rainstorm.Message response = NetworkUtils.sendAndReceiveProto(task.getIp(), task.getPort(), message);
                    if (response != null && response.getType() == Rainstorm.Type.EOF_ACK) {
                        acked = true;
                        break;
                    }
                    logger.warn("Invalid response from task {}, attempt {}", task.getId(), attempt);
                } catch (Exception e) {
                    logger.warn("EOF send failed to task {} on attempt {}: {}", task.getId(), attempt, e.getMessage());
                }
                long delay = Math.min(maxDelayMs, baseDelayMs * (1L << attempt));
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
                attempt++;
            }
            if (!acked)
                logger.error("Task {} FAILED to ACK EOF after {} retries", task.getId(), maxRetries);
        }
        logger.info("[EOF] sent EOF to all tasks");
    }

    protected void handleTupleMessage(Rainstorm.Message message) {
        //do nothing
    }

    private void flushLogAndSendBuffer() {
        List<TupleState> batch = new ArrayList<>();
        long lastFlush = System.currentTimeMillis();
        final int BATCH_SIZE = BUFFER_SIZE;
        final int MAX_WAIT_MS = 500;
        try {
            while (running) {
                TupleState tp = logAndSendBuffer.poll(10, TimeUnit.MILLISECONDS);
                if (tp != null) batch.add(tp);
                boolean batchFull = batch.size() >= BATCH_SIZE;
                boolean timeExceeded = System.currentTimeMillis() - lastFlush >= MAX_WAIT_MS;
                if ((batchFull || timeExceeded) && !batch.isEmpty()) {
                    processLogAndSendBatch(batch);
                    batch.clear();
                    lastFlush = System.currentTimeMillis();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void flushAckReceivedBuffer() {
        List<TupleState> batch = new ArrayList<>();
        long lastFlush = System.currentTimeMillis();
        final int BATCH_SIZE = BUFFER_SIZE;
        final int MAX_WAIT_MS = 500;
        try {
            while (running) {
                TupleState tp = ackReceivedBuffer.poll(10, TimeUnit.MILLISECONDS);
                if (tp != null) batch.add(tp);
                boolean batchFull = batch.size() >= BATCH_SIZE;
                boolean timeExceeded = System.currentTimeMillis() - lastFlush >= MAX_WAIT_MS;
                if ((batchFull || timeExceeded) && !batch.isEmpty()) {
                    processAckReceivedBatch(batch);
                    batch.clear();
                    lastFlush = System.currentTimeMillis();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void processAckReceivedBatch(List<TupleState> batch) {
        StringBuilder append = new StringBuilder();
        for (TupleState tuple : batch) {
            LogEntry logEntry = new LogEntry(LogEntry.Type.ACK_RECEIVED, tuple.tupleId);
            append.append(logEntry).append("\n");
        }
        try {
            logToDisk(append);
        } catch (IOException ignored) {
        }
    }

    private void processLogAndSendBatch(List<TupleState> batch) {
        logger.info("[LOGGING] processing log and send batch");
        StringBuilder append = new StringBuilder();
        for (TupleState tuple : batch) {
            for (TupleState.TupleOutput output : tuple.outputs) {
                LogEntry logEntry = new LogEntry(LogEntry.Type.OUTPUT, tuple.tupleId);
                logEntry.fields.put(LogEntryField.TO_TASK, output.taskId);
                append.append(logEntry).append("\n");
            }
        }
        try {
            logToDisk(append);
            for (TupleState tuple : batch) {
                for (TupleState.TupleOutput output : tuple.outputs) {
                    Rainstorm.Task task;
                    if (tasks.containsKey(output.taskId))
                        task = tasks.get(output.taskId);
                    else {
                        int hash = tuple.processedKey.hashCode();
                        int outputTaskIndex = Math.abs(hash) % nextTasks.size();
                        task = nextTasks.get(outputTaskIndex);
                        output.taskId = task.getId();
                    }
                    sendTuple(tuple.tupleId, output.taskId, task.getIp(), task.getPort(), tupleMessage(buildTuple(tuple.tupleId, tuple.processedKey, tuple.processedValue)), tuple);
                }
            }
        } catch (IOException e) {
            logger.error("Error logging to disk, re-adding all tuples for logAndSend batch");
            logAndSendBuffer.addAll(batch);
        }
    }

    private void resendTuples() {
        logger.info("Resending tuples");
        try {
            for (TupleState tuple : pending.values()) {
                for (TupleState.TupleOutput tupleOutput : tuple.outputs) {
                    if (tuple.output && !tupleOutput.ackReceived && tuple.processedKey != null) {
                        Rainstorm.Task task;
                        if (tasks.containsKey(tupleOutput.taskId))
                            task = tasks.get(tupleOutput.taskId);
                        else {
                            int hash = tuple.processedKey.hashCode();
                            int outputTaskIndex = Math.abs(hash) % nextTasks.size();
                            task = nextTasks.get(outputTaskIndex);
                            tupleOutput.taskId = task.getId();
                        }
                        logger.info("[RESEND] Resending tuple: {}", tuple.tupleId);
                        sendTuple(tuple.tupleId, tupleOutput.taskId, task.getIp(), task.getPort(), tupleMessage(buildTuple(tuple.tupleId, tuple.processedKey, tuple.processedValue)), tuple);
                    } else if (tupleOutput.ackReceived) {
                        logger.info("[RESEND] Removing {} from pending, ACK received", tuple.tupleId);
                        pending.remove(tuple.tupleId);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("[RESEND_ERROR]: {}", e.getMessage(), e);
        }
    }

    public String getTaskId() {
        return taskId;
    }

    public int getMyPort() {
        return port;
    }

    private Rainstorm.Tuple buildTuple(String id, String key, String value) {
        return Rainstorm.Tuple.newBuilder()
                .setId(id)
                .setKey(key)
                .setValue(value)
                .setSenderTaskId(taskId)
                .build();
    }

    public void emitTuple(String tupleId, String key, String value, boolean skipLogging) {
        Rainstorm.Tuple tuple = buildTuple(tupleId, key, value);
        int hash = tuple.getKey().hashCode();
        // nextTasks can NOT be empty
        //TODO if nextTasks is empty, handle it separately (extract method)
        if (nextTasks.isEmpty()) {
            outputToHydfsBuffer.add(tuples.get(tupleId));
        } else {
            int outputTaskIndex = Math.abs(hash) % nextTasks.size();
            Rainstorm.Task outputTask = nextTasks.get(outputTaskIndex);
            TupleState ts = tuples.computeIfAbsent(tupleId, TupleState::new);
            ts.output = true;
            ts.outputs.add(new TupleState.TupleOutput(outputTask.getId()));
            if (skipLogging) {
                sendTuple(tupleId, outputTask.getId(), outputTask.getIp(), outputTask.getPort(), tupleMessage(tuple), ts);
            } else {
                logAndSendBuffer.add(tuples.get(tupleId));
            }
        }
    }

    public void sendTuple(String tupleId, String taskId, String host, int port, Rainstorm.Message message, TupleState ts) {
        try (Socket socket = new Socket(host, port)) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            message.writeDelimitedTo(out);
            out.flush();
            pending.putIfAbsent(tupleId, ts);
            logger.info("Sent tuple: {} to VM {}:{}", tupleId, host, port);
        } catch (Exception e) {
            pending.putIfAbsent(tupleId, ts);
            logger.error("Error sending tuple: {} with key: {} to taskID: {}, e: {}", tupleId, message.getTuple().getKey(), taskId, e.getMessage());
        }
    }

    protected void notifyded() {
        try {
            Rainstorm.ControlMessage msg =
                    MessageUtils.buildTaskEndedMessage(
                            taskId,
                            metadata.getMyTask().getStageValue(),
                            metadata.getMyTask().getIp());
            NetworkUtils.sendProtoMessage(leaderIp, leaderPort, msg);


            System.out.println("[Task " + taskId + "] Sent TaskEnded to leader.");

        } catch (Exception e) {
            System.err.println("[Task " + taskId + "] ERROR sending TaskEnded: " + e.getMessage());
        }
    }


    public abstract void start();

    public void stop() {
        if (dataListenerSocket != null && !dataListenerSocket.isClosed()) {
            try {
                dataListenerSocket.close();
                logger.info("Closed data listener socket - no longer accepting connections");
            } catch (IOException e) {
                logger.error("Error closing data listener socket: {}", e.getMessage());
            }
        }
        logger.info("[STOP] Stopped listening to new tuples");
    }

    protected void sendStoppedMessage() {
        Rainstorm.ControlMessage message = MessageUtils.buildTaskEndedMessage(taskId, stage, metadata.getMyTask().getIp());
        NetworkUtils.sendProtoMessage(metadata.getMyTask().getIp(), WORKER_PORT, message);
        logger.info("[STOP] Sent stopped message to worker");
    }
}

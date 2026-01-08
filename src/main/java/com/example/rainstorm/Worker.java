package com.example.rainstorm;

import com.example.MessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rainstorm.Rainstorm;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {
    private final Logger logger = LoggerFactory.getLogger(Worker.class);
    private final int controlPort;
    private final Map<String, Rainstorm.Metadata> taskMap = new ConcurrentHashMap<>();
    private final ExecutorService taskExecutor = Executors.newCachedThreadPool();
    private final ExecutorService listenerPool = Executors.newCachedThreadPool();
    private TaskSupervisor supervisor = null;
    private volatile boolean autoscaleEnabled = false;
    private boolean isSupervisorRunning = false;

    public Worker(int controlPort) {
        this.controlPort = controlPort;
    }

    public void start() {
        startControlListener();
        supervisor = new TaskSupervisor(taskMap);
        System.out.println("[Worker] Control listener + supervisor started.");
    }

    private void startControlListener() {
        listenerPool.submit(() -> {
            try (ServerSocket server = new ServerSocket(controlPort)) {
                while (true) {
                    Socket socket = server.accept();
                    handleControlMessage(socket);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void handleControlMessage(Socket socket) {
        try {
            Rainstorm.ControlMessage msg = Rainstorm.ControlMessage.parseDelimitedFrom(socket.getInputStream());
            if (msg == null) return;
            else if (msg.hasStop()) {
                handleStopTask(msg.getStop().getTaskId());
            } else if (msg.hasStopAllTasks()) {
                handleStopAllTasks();
            } else if (msg.hasEnded()) {
                String taskId = msg.getEnded().getTaskId();
                handleStopTask(taskId);
                System.out.println("[Worker] Task ended notification received for taskId: " + taskId);

                System.out.println("[Worker] Task " + taskId + " removed from taskMap.");

            } else if (msg.hasKillTask()) {
                int pid = msg.getKillTask().getPid();
                handleKillTask(pid);
            } else if (msg.hasListReq()) {
                sendTaskListToLeader(socket);
            } else if (msg.hasPayload()) {
                Rainstorm.ControlPayload payload = msg.getPayload();
                Rainstorm.Type type = payload.getType();
                switch (type) {
                    case TASK_METADATA:
                        handleMetadataUpdate(payload);
                        if (!isSupervisorRunning && !autoscaleEnabled) {
                            isSupervisorRunning = true;
                            supervisor.start();
                        }
                        break;
                    default:
                        System.err.println("[Worker] Unknown control payload type: " + type);
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendStopTask(String taskId) {
        Rainstorm.Metadata md = taskMap.get(taskId);
        if (md == null) {
            System.err.println("[Worker] sendStopTask: Task " + taskId + " not found in taskMap");
            return;
        }

        Rainstorm.Task t = md.getMyTask();
        String taskIp = t.getIp();
        int dataPort = t.getPort();

        // Build STOP message
        Rainstorm.Message stopMsg = MessageUtils.buildStopMessage();

        System.out.println("[Worker] Sending STOP to task " + taskId + " at " + taskIp + ":" + dataPort);

        try {
            NetworkUtils.sendProtoMessage(taskIp, dataPort, stopMsg);
        } catch (Exception e) {
            System.err.println("[Worker] Failed to send STOP to task " + taskId + ": " + e.getMessage());
        }
    }

    public void handleMetadataUpdate(Rainstorm.ControlPayload payload) {
        autoscaleEnabled = payload.getAutoscale();
        Set<String> tasksInPayload = new HashSet<>();
        for (Rainstorm.Metadata metadata : payload.getMetadataList()) {
            tasksInPayload.add(metadata.getMyTask().getId());
            if (!taskMap.containsKey(metadata.getMyTask().getId())) {
                spawnTask(metadata, payload);
            } else if (!isSame(metadata, taskMap.get(metadata.getMyTask().getId()))) {
                updateTaskMetadata(metadata, payload);
            }
        }
        for (String task : taskMap.keySet()) {
            if (!tasksInPayload.contains(task)) {
                sendStopTask(task);
            }
        }
    }

    private void updateTaskMetadata(Rainstorm.Metadata metadata, Rainstorm.ControlPayload payload) {
        Rainstorm.Message message = Rainstorm.Message.newBuilder()
                .setType(Rainstorm.Type.TASK_METADATA)
                .setMetadata(metadata)
                .addAllTasks(payload.getAllTasksList())
                .build();
        taskMap.put(metadata.getMyTask().getId(), metadata);
        NetworkUtils.sendProtoMessage(metadata.getMyTask().getIp(), metadata.getMyTask().getPort(), message);
        logger.info("Sent TASK_METADATA_UPDATE to task: {}", metadata.getMyTask().getId());
    }

    private boolean isSame(Rainstorm.Metadata newMetadata, Rainstorm.Metadata oldMetadata) {
        if (newMetadata.getNextTasksCount() != oldMetadata.getNextTasksCount()) {
            return false;
        } else {
            for (Rainstorm.Task oldTask : oldMetadata.getNextTasksList()) {
                Optional<Rainstorm.Task> newTask = newMetadata.getNextTasksList().stream().filter(t -> t.getId().equals(oldTask.getId())).findFirst();
                if (newTask.isEmpty()) return false;
                if (!newTask.get().getIp().equals(oldTask.getIp())) return false;
            }
        }
        logger.info("FOUND_SAME_METADATA");
        logger.info("OLD_TASKS: {}", oldMetadata.getNextTasksList());
        logger.info("NEW_TASKS: {}", newMetadata.getNextTasksList());
        return true;
    }


    private void spawnTask(Rainstorm.Metadata metadata, Rainstorm.ControlPayload payload) {
        Rainstorm.ControlPayload newPayload = Rainstorm.ControlPayload.newBuilder()
                .addMetadata(metadata)
                .setAutoscale(payload.getAutoscale())
                .setType(payload.getType())
                .addAllAllTasks(payload.getAllTasksList())
                .build();
        String payloadB64 = Base64.getEncoder().encodeToString(newPayload.toByteArray());
        String taskId = metadata.getMyTask().getId();
        String logFile = metadata.getMyTask().getLogFile();

        String projectRoot = System.getProperty("user.dir");

        ProcessBuilder pb = new ProcessBuilder(
                "java",
                "-Dlogfile=" + "task_" + taskId + ".log",
                "-cp", projectRoot + "/target/rainstorm-1.0-SNAPSHOT.jar",
                "com.example.rainstorm.TaskRunner",
                metadata.getMyTask().getStage().equals(Rainstorm.Stage.SOURCE) ? "0" : "1",
                payloadB64,
                taskId,
                logFile
        );

        try {
            Process p = pb.start();
            Rainstorm.Task newTask = metadata.getMyTask().toBuilder().setPid(p.pid()).build();
            Rainstorm.Metadata newMetadata = metadata.toBuilder().setMyTask(newTask).build();
            taskMap.put(metadata.getMyTask().getId(), newMetadata);
            System.out.println("Started Task with ID: " + metadata.getMyTask().getId() + ", PID: " + p.pid());
        } catch (Exception e) {
            System.out.println("Failed to start task: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleKillTask(long pid) {
        System.out.println("[Worker] KillTask received for PID = " + pid);

        // Lookup which task has this PID
        Rainstorm.Metadata toKill = null;

        for (Rainstorm.Metadata md : taskMap.values()) {
            if (md.getMyTask().getPid() == pid) {
                toKill = md;
                break;
            }
        }
        if (toKill == null) {
            System.err.println("[Worker] ERROR: No task found with PID " + pid);
            return;
        }
        // Kill OS process
        ProcessHandle handle = ProcessHandle.of(pid).orElse(null);

        if (handle == null || !handle.isAlive()) {
            System.err.println("[Worker] ERROR: Process " + pid + " not alive.");
        } else {
            System.out.println("[Worker] Forcibly killing PID " + pid);
            handle.destroyForcibly();
        }
        System.out.println("[Worker] Task " + toKill.getMyTask().getId() + " killed and removed.");
    }


    private void sendTaskListToLeader(Socket socket) {
        try {
            // --- 1. Build TaskEntry list from taskMap ---
            List<Rainstorm.Task> taskEntries = new ArrayList<>();
            for (Rainstorm.Metadata md : taskMap.values()) {
                Rainstorm.Task t = md.getMyTask();
                taskEntries.add(t);
            }
            Rainstorm.ControlMessage resp = MessageUtils.buildTaskListMessage(taskEntries);
            resp.writeDelimitedTo(socket.getOutputStream());
            socket.getOutputStream().flush();
            System.out.println("[Worker] Sent TaskList response to Leader. Entries=" + taskEntries.size());
        } catch (Exception e) {
            System.err.println("[Worker] Failed to send TaskList to Leader: " + e);
        }
    }

    private void handleStopTask(String taskId) {
        System.out.println("[Worker] StopTask received for taskId: " + taskId);

        Rainstorm.Metadata taskMetadata = taskMap.get(taskId);
        if (taskMetadata == null) {
            System.err.println("[Worker] ERROR: Task " + taskId + " not found in taskMap");
            return;
        }
        long pid = taskMetadata.getMyTask().getPid();

        ProcessHandle handle = ProcessHandle.of(pid).orElse(null);
        if (handle == null || !handle.isAlive()) {
            System.out.println("[Worker] Task " + taskId + " (PID " + pid + ") is not alive, removing from map");
            taskMap.remove(taskId);
            return;
        }
        // Send graceful shutdown signal (SIGTERM)
        System.out.println("[Worker] Sending graceful shutdown signal to task " + taskId + " (PID " + pid + ")");
        handle.destroy();  // Graceful shutdown

        // Wait for process to finish gracefully
        final long GRACEFUL_TIMEOUT_MS = 10000;  // 10 seconds
        final long startTime = System.currentTimeMillis();

        try {
            while (handle.isAlive() && (System.currentTimeMillis() - startTime) < GRACEFUL_TIMEOUT_MS) {
                Thread.sleep(100);  // Check every 100ms
            }

            if (handle.isAlive()) {

                System.out.println("[Worker] Task " + taskId + " did not exit gracefully within timeout, force killing...");
                handle.destroyForcibly();
            }

            Thread.sleep(500);
            System.out.println("[Worker] Task " + taskId + " exited gracefully");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[Worker] Interrupted while waiting for task " + taskId + " to stop");
            if (handle.isAlive()) {
                handle.destroyForcibly();
            }
        }
        taskMap.remove(taskId);
        System.out.println("[Worker] Task " + taskId + " stopped and removed from taskMap");

    }

    public void handleStopAllTasks() {
        System.out.println("[Worker] StopAllTasks received. Stopping all tasks...");
        for (String taskId : new ArrayList<>(taskMap.keySet())) {
            handleStopTask(taskId);
        }
        System.out.println("[Worker] All tasks stopped.");
    }
}

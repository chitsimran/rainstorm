package com.example.rainstorm;

import com.example.MessageUtils;
import com.example.constants.AppConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rainstorm.Rainstorm;

import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaskSupervisor {
    private static final int FAIL_THRESHOLD = 2;
    private final Map<String, Rainstorm.Metadata> taskMap;
    private final Map<String, TaskHealth> healthMap = new ConcurrentHashMap<>();
    private final String leaderIp = AppConstants.LEADER_IP;
    private final int leaderPort = AppConstants.LEADER_CONTROL_PORT;
    Logger logger = LoggerFactory.getLogger(TaskSupervisor.class);
    private volatile boolean running = true;

    public TaskSupervisor(Map<String, Rainstorm.Metadata> taskMap) {
        this.taskMap = taskMap;
    }

    public void start() {
        try {
            Thread.sleep(2000);
        } catch (Exception ignored) {
        }
        new Thread(this::runLoop, "TaskSupervisor").start();
    }

    private void runLoop() {
        while (running) {
            try {
                // Sleep if no tasks yet
                if (this.taskMap.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }

                monitorAllTasks();
                Thread.sleep(1000);

            } catch (Exception ignored) {
            }
        }
    }

    private void monitorAllTasks() {
        // Remove health for tasks no longer running
        healthMap.keySet().removeIf(taskId -> !taskMap.containsKey(taskId));
        // Check liveness for each currently-running task
        for (Rainstorm.Metadata md : taskMap.values()) {
            String taskId = md.getMyTask().getId();
            TaskHealth h = healthMap.computeIfAbsent(taskId, k -> new TaskHealth());
            boolean alive = pingTask(md);
            if (alive) {
                h.consecutiveFailures = 0;
                if (!h.alive) {
                    logger.info("[Supervisor] Task recovered: " + taskId);
                    h.alive = true;
                }
            } else {
                h.consecutiveFailures++;
                if (h.consecutiveFailures >= FAIL_THRESHOLD && h.alive) {
                    h.alive = false;
                    System.err.println("[Supervisor] DEAD: " + taskId);
                    notifyLeader(md);
                }
            }
        }
    }

    private boolean pingTask(Rainstorm.Metadata md) {
        Rainstorm.Task t = md.getMyTask();
        try (Socket socket = new Socket("localhost", t.getPort())) {
            Rainstorm.Message ping = MessageUtils.buildTaskPingMessage(t.getId());
            ping.writeDelimitedTo(socket.getOutputStream());
            socket.setSoTimeout(400);
            Rainstorm.ControlMessage resp = Rainstorm.ControlMessage.parseDelimitedFrom(socket.getInputStream());
            return resp != null && resp.hasAck();
        } catch (Exception e) {
            logger.error("Ping failed for task: {}, e: {}", t.getId(), e.getMessage());
            return false; // treat all exceptions as failed pings
        }
    }

    private void notifyLeader(Rainstorm.Metadata md) {
        Rainstorm.Task t = md.getMyTask();
        Rainstorm.ControlMessage msg = MessageUtils.buildFailureReportMessage(
                t.getId(),
                t.getStageValue()
        );
        taskMap.remove(t.getId());
        NetworkUtils.sendProtoMessage(leaderIp, leaderPort, msg);
        System.err.println("[Supervisor] Failure report sent for task " + t.getId());
    }
}



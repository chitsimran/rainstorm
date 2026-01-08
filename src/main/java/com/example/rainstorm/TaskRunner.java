package com.example.rainstorm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rainstorm.Rainstorm;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TaskRunner {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(TaskRunner.class);
        try {
            logger.info("In task runner");
            String stage = args[0];
            String payloadB64 = args[1];
            String taskId = args[2];
            String logFile = args[3];
            logger.info("stage: {}, taskId: {}, logFile: {}", stage, taskId, logFile);
            logger.info("payload: {}", payloadB64);
            byte[] payload = Base64.getDecoder().decode(payloadB64);
            Rainstorm.ControlPayload controlPayload = Rainstorm.ControlPayload.parseFrom(payload);
            logger.info("payload: {}", controlPayload);
            Map<String, Rainstorm.Task> tasks = new HashMap<>();
            for (Rainstorm.Task task : controlPayload.getAllTasksList()) {
                tasks.put(task.getId(), task);
            }
            Task task;
            if (Objects.equals(stage, "0"))
                task = new SourceStreamTask(controlPayload.getMetadata(0), tasks, taskId, logFile, controlPayload.getAutoscale());
            else
                task = new WorkerTask(controlPayload.getMetadata(0), tasks, taskId, logFile, controlPayload.getAutoscale());
            task.start();
            logger.info("Started task: {}", taskId);
        } catch (Exception e) {
            logger.error("Failed to run task: {}", e.getMessage());
        }
    }
}

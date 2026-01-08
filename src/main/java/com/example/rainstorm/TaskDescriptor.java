package com.example.rainstorm;

public class TaskDescriptor {
    public final int stage;          // Stage number: 0 = SOURCE, 1..N operators
    public final int taskIndex;      // Task index within stage
    public final String opExe;       // Operator executable
    public final String taskId;         // Unique task ID
    public String nodeId;            // Worker node ID
    public String ip;                // Worker VM IP
    public int controlPort;          // Control port for receiving commands
    public int dataPort;             // Data port for receiving tuples
    public String opArgs;      // Operator arguments
    public String logfile;
    public String sourceFile;        // Source file only for stage 0
    public String destinationFile;
    public int inputRate;
    public String addOpArgs;
    public double rate;

    public TaskDescriptor(
            int stage,
            int taskIndex,
            String nodeId,
            String ip,
            int controlPort,
            int dataPort,
            String opExe,
            String opArgs,
            String taskId,
            String logfile,
            String destinationFile,
            int inputRate) {
        this.stage = stage;
        this.taskIndex = taskIndex;

        this.nodeId = nodeId;
        this.ip = ip;

        this.controlPort = controlPort;
        this.dataPort = dataPort;

        this.opExe = opExe;
        this.opArgs = opArgs;
        this.taskId = taskId;
        this.sourceFile = "";
        this.logfile = logfile;
        this.destinationFile = destinationFile;
        this.inputRate = inputRate;
        this.addOpArgs = "";
    }

    @Override
    public String toString() {
        return String.format(
                "TaskDescriptor(stage=%d, task=%d, node=%s, ip=%s, ctrl=%d, data=%d, exe=%s, args=%s)",
                stage, taskIndex, nodeId, ip, controlPort, dataPort, opExe, opArgs
        );
    }
}


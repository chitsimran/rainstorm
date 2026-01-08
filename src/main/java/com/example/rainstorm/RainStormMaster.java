package com.example.rainstorm;

import com.example.Member;
import com.example.MessageUtils;
import com.example.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rainstorm.Rainstorm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.example.constants.AppConstants.WORKER_PORT;

public class RainStormMaster {
    private static final int VM_CONTROL_PORT = WORKER_PORT;
    public final Topology topology = new Topology();
    private final Node node;
    private final int numStages;
    private final int tasksPerStage;
    private final List<String> opExecList;      // op1_exe, op2_exe, ...
    private final List<String> opArgsList;// op1_args, op2_args,...
    private final String hydfsSrcDir;
    private final String hydfsDestFile;
    private final boolean exactlyOnce;
    private final boolean autoscale;
    private final int inputRate;
    private final int LOAD_PORT = 9100;
    private final int LW, HW;
    private final String invocationId;
    private final Map<Integer, List<TaskDescriptor>> routingMap = new HashMap<>();
    private final Logger logger = LoggerFactory.getLogger(RainStormMaster.class);
    public Map<Integer, Map<Integer, Double>> stageTaskRates = new ConcurrentHashMap<>();
    Map<Integer, Integer> counter = new HashMap<>();
    private AutoScaler autoscaler;
    private ReportListener reportListener;
    private Member sourceNode;
    private List<Member> aliveNodes;
    private int rrCounter = 0;


    public RainStormMaster(int numStages, int tasksPerStage, List<String> opExecList, List<String> opArgsList, String hydfsSrcDir, String hydfsDestFile, boolean exactlyOnce, boolean autoscale, int inputRate, int LW, int HW, Node node) {
        this.numStages = numStages;
        this.tasksPerStage = tasksPerStage;

        this.opExecList = opExecList;
        this.opArgsList = opArgsList;

        this.hydfsSrcDir = hydfsSrcDir;
        this.hydfsDestFile = hydfsDestFile;

        this.exactlyOnce = exactlyOnce;
        this.autoscale = autoscale;

        this.inputRate = inputRate;
        this.LW = LW;
        this.HW = HW;
        this.invocationId = Long.toHexString(System.currentTimeMillis() & 0xFFFFF);

        this.node = node;
        this.aliveNodes = node.getAliveNodes();
    }

    public void start() {
        System.out.println("[RainStorm] Leader starting...");
        List<Member> aliveNodes = node.getAliveNodes();
        if (aliveNodes.size() < 2) {
            System.err.println("[RainStorm] Need at least 2 nodes (1 leader + 1 worker)!");
            return;
        }
        createEmptyHyDFSLogFile(hydfsDestFile);
        // 1. Build topology
        assignTasksToWorkers(aliveNodes);
        // 2. Build routing tables
        buildRoutingTables();
        startAllTasks();

        //  Start ReportListener thread
        reportListener = new ReportListener(this, LOAD_PORT);  //
        Thread t = new Thread(reportListener, "ReportListener");
        t.start();

        if (autoscale) {
            autoscaler = new AutoScaler(this, numStages, LW, HW);
            new Thread(autoscaler).start();
        }

        System.out.println("[RainStorm] All tasks started successfully.");
    }

    private void assignTasksToWorkers(List<Member> aliveNodes) {
        if (aliveNodes.isEmpty()) {
            System.err.println("[RainStorm] ERROR: No alive workers available.");
            return;
        }
        // Initialize topology for stages 0..numStages
        topology.initStages(numStages);

        // ASSIGN SOURCE TASK (Stage 0)
        this.sourceNode = aliveNodes.get(0);
        String srcTaskId = invocationId + "_0_0";
        String srcLog = createEmptyHyDFSLogFile(srcTaskId);

        TaskDescriptor sourceTd = new TaskDescriptor(0,                        // stage
                0,                        // taskIndex
                sourceNode.nodeId, sourceNode.host, sourceNode.port,          // control port
                sourceNode.port + 100,          // data port
                "",                       // no opExe for source
                "",                // no opArgs
                srcTaskId, srcLog,                    // logfile
                hydfsDestFile, inputRate
        );

        topology.addTask(0, sourceTd);

        List<Member> workerNodes = aliveNodes.subList(1, aliveNodes.size());
        int numWorkers = workerNodes.size();

        for (int stage = 1; stage <= numStages; stage++) {
            String opExe = opExecList.get(stage - 1);
            String opArgs = opArgsList.get(stage - 1);
            for (int t = 0; t < tasksPerStage; t++) {
                Member assigned = getNextVM();
                String taskId = invocationId + "_" + stage + "_" + getAndIncrement(stage);
                String logFile = createEmptyHyDFSLogFile(taskId);
                TaskDescriptor td = new TaskDescriptor(stage, t, assigned.nodeId, assigned.host, assigned.port, assigned.port + (stage * 10) + t, opExe, opArgs, taskId, logFile, hydfsDestFile, inputRate);
                topology.addTask(stage, td);
            }
        }
        System.out.println("[RainStorm] Topology built:");
        topology.printTopology();
    }

    public int getAndIncrement(int stage) {
        int current = counter.getOrDefault(stage, 0);
        counter.put(stage, current + 1);
        return current;
    }

    private void buildRoutingTables() {

        routingMap.clear();

        for (int stage = 1; stage <= numStages; stage++) {

            if (stage == numStages) {
                routingMap.put(stage, Collections.emptyList());
                continue;
            }
            List<TaskDescriptor> nextStageTasks = topology.getTasks(stage + 1);
            routingMap.put(stage, nextStageTasks);
        }
    }

    public void startAllTasks() {

        System.out.println("[RainStorm] === Starting All Tasks ===");

        // Send metadata for operator tasks to all VMs
        sendOperatorMetadataToAllVMs(Rainstorm.Type.TASK_METADATA, true);

        try {
            Thread.sleep(700);
        } catch (InterruptedException ignored) {
        }

        //  Start source
        startSourceTask();

        System.out.println("[RainStorm] All tasks started successfully.");
    }

    private void sendOperatorMetadataToAllVMs(Rainstorm.Type type, boolean skipSource) {
        List<Rainstorm.Task> allTasksList = buildAllTasksList();
        Map<String, List<TaskDescriptor>> vmAssignments = topology.groupTasksByVm();

        for (Map.Entry<String, List<TaskDescriptor>> entry : vmAssignments.entrySet()) {

            String vmIp = entry.getKey();
            List<TaskDescriptor> taskList = entry.getValue();

            List<Rainstorm.Metadata> metadataList = new ArrayList<>();

            for (TaskDescriptor td : taskList) {
                if (td.stage == 0 && skipSource) continue;   // Skip SOURCE here
                metadataList.add(metadataFor(td));
            }

            if (!metadataList.isEmpty()) {
                Rainstorm.ControlMessage msg = MessageUtils.buildControlPayloadMessage(metadataList, autoscale, type, allTasksList);
                NetworkUtils.sendProtoMessage(vmIp, VM_CONTROL_PORT, msg);
            }
        }
    }

    private void startSourceTask() {
        List<Rainstorm.Task> allTasksList = buildAllTasksList();
        TaskDescriptor sourceTd = topology.getTasks(0).get(0);
        sourceTd.sourceFile = hydfsSrcDir; // assign dataset file
        Rainstorm.Metadata sourceMd = metadataFor(sourceTd);
        Rainstorm.ControlMessage sourceInit = MessageUtils.buildControlPayloadMessage(List.of(sourceMd), autoscale, Rainstorm.Type.TASK_METADATA, allTasksList);
        NetworkUtils.sendProtoMessage(sourceTd.ip, VM_CONTROL_PORT, sourceInit);
        System.out.println("[RainStorm] SOURCE started on " + sourceTd.ip);
    }

    private Rainstorm.Metadata metadataFor(TaskDescriptor td) {

        if (td.stage >= 1 && td.stage < numStages) {         // skip source(0) and last stage
            int nextStage = td.stage + 1;

            if (isStatefulStage(nextStage)) {
                // nextStage's index in opArgsList is (nextStage - 1)
                String aggArgs = opArgsList.get(nextStage - 1);
                td.addOpArgs = aggArgs;
            } else {
                td.addOpArgs = "NONE";
            }
        }

        //  Convert task to proto
        Rainstorm.Task myTaskProto = MessageUtils.convertToProto(td);

        // Find next stage tasks
        int nextStage = td.stage + 1;
        List<TaskDescriptor> nextStageTasks = topology.getTasks(nextStage);

        List<Rainstorm.Task> nextTaskProtos = new ArrayList<>();
        if (nextStageTasks != null) {
            for (TaskDescriptor nextTd : nextStageTasks) {
                nextTaskProtos.add(MessageUtils.convertToProto(nextTd));
            }
        }
        return Rainstorm.Metadata.newBuilder()
                .setMyTask(myTaskProto)
                .addAllNextTasks(nextTaskProtos)
                .build();
    }

    private List<Rainstorm.Task> buildAllTasksList() {
        List<Rainstorm.Task> all = new ArrayList<>();

        for (int stage = 0; stage <= numStages; stage++) {
            List<TaskDescriptor> tds = topology.getTasks(stage);
            if (tds == null) continue;

            for (TaskDescriptor td : tds) {
                all.add(MessageUtils.convertToProto(td));
            }
        }
        return all;
    }

    public Map<Integer, Double> queryStageLoads() {
        Map<Integer, Double> result = new HashMap<>();

        for (int stage = 1; stage <= numStages; stage++) {
            List<TaskDescriptor> tasks = topology.getTasks(stage);
            int totalCount = 0;
            double totalRate = 0;
            for (TaskDescriptor taskDescriptor : tasks) {
                totalRate += taskDescriptor.rate;
                totalCount++;
            }
            if (totalCount == 0) {
                result.put(stage, 0.0);
            } else {
                result.put(stage, totalRate / totalCount);
            }
        }
        return result;
    }

    public Member getNextVM() {
        List<Member> alive = node.getAliveNodes();
        if (alive == null || alive.isEmpty()) return null;
        int n = alive.size();
        if (Objects.equals(alive.get(rrCounter).nodeId, sourceNode.nodeId)) {
            rrCounter = (rrCounter + 1) % n;
        }
        Member chosen = alive.get(rrCounter);
        rrCounter = (rrCounter + 1) % n;
        return chosen;
    }

    public void scaleUp(int stage, Member assignedVM, double measuredRate) {
        System.out.println("[AutoScaler] UPSCALING stage " + stage + " on VM " + assignedVM.host +
                " | measured rate: " + String.format("%.2f", measuredRate) + " tuples/sec per task");
        int newTaskIndex = getAndIncrement(stage);
        String opExe = opExecList.get(stage - 1);
        String opArgs = opArgsList.get(stage - 1);
        String taskId = invocationId + "_" + stage + "_" + newTaskIndex;
        String logFile = createEmptyHyDFSLogFile(taskId);
        TaskDescriptor td = new TaskDescriptor(stage, newTaskIndex, assignedVM.nodeId, assignedVM.host, assignedVM.port, assignedVM.port + (stage * 10) + newTaskIndex, opExe, opArgs, taskId, logFile, hydfsDestFile, inputRate);
        topology.addTask(stage, td);
        sendOperatorMetadataToAllVMs(Rainstorm.Type.TASK_METADATA, false);
        logger.info("[AutoScaler] scaleUp complete: added taskIndex={} to stage {}", newTaskIndex, stage);
    }

    public void scaleDown(int stage, double measuredRate) {
        System.out.println("[AutoScaler] DOWNSCALING stage " + stage + " | measured rate: " + String.format("%.2f", measuredRate) + " tuples/sec per task");
        int count = topology.getNumTasks(stage);
        if (count <= 1) {
            System.out.println("[AutoScaler] Cannot scale down stage " + stage + " below 1 task");
            return;
        }
        int taskToRemove = count - 1;
        // Remove the last task
        topology.removeLastTask(stage);
        sendOperatorMetadataToAllVMs(Rainstorm.Type.TASK_METADATA, false);
        logger.info("[AutoScaler] scaleDown complete: removed taskIndex={} from stage {}", taskToRemove, stage);
    }

    public void sendStopTask(String taskId, String ip, int stage) {
        Rainstorm.ControlMessage stopMsg = MessageUtils.buildStopTaskMessage(taskId);
        NetworkUtils.sendProtoMessage(ip, VM_CONTROL_PORT, stopMsg);
        topology.removeTaskById(stage, taskId);
        buildRoutingTables();
        System.out.println("[Master] StopTask sent and topology updated.");
    }

    public void handleTaskFailure(Rainstorm.FailureReport fr) {
        int stage = fr.getStage();
        String taskId = fr.getTaskId();
        System.err.println("[Leader] FAILURE detected: taskId=" + taskId + " stage=" + stage);

        TaskDescriptor failed = topology.findTaskById(stage, taskId);
        if (failed == null) {
            System.err.println("[Leader] Warning: failed task not found in topology.");
            return;
        }

        // 2. Choose a VM to restart on
        Member newVm = getNextVM();
        if (newVm == null) {
            System.err.println("[Leader] No alive VMs available to restart task " + taskId);
            return;
        }
        TaskDescriptor replacement = new TaskDescriptor(
                stage,
                failed.taskIndex,        // keep same index
                newVm.nodeId, newVm.host, newVm.port,              // control port
                newVm.port + (stage * 10) + failed.taskIndex,              // data port
                failed.opExe,            // same operator binary
                failed.opArgs,           // same args
                failed.taskId,           // SAME taskId
                failed.logfile,           // SAME HyDFS log file for exactly-once
                failed.destinationFile,
                failed.inputRate
        );


        //  Replace in topology
        topology.replaceTask(stage, failed.taskIndex, replacement);

        sendOperatorMetadataToAllVMs(Rainstorm.Type.TASK_METADATA, false);
        System.out.println("[Leader] Restarted failed task " + taskId + " on VM " + newVm.nodeId);
    }

    private String createEmptyHyDFSLogFile(String fileName) {
        String dfsPath = fileName;
        try {
            byte[] empty = new byte[0];
            //TODO create file with dfsPath on distributed file system
            logger.info("[Leader] Created empty log file on HyDFS: " + dfsPath);
        } catch (Exception e) {
            System.err.println("[Leader] ERROR creating log file on HyDFS: " + dfsPath);
            e.printStackTrace();
        }
        return dfsPath;
    }

    public void listTasks() {
        System.out.println("=== LIST_TASKS ===");

        List<Member> alive = node.getAliveNodes();

        for (Member m : alive) {

            Rainstorm.ControlMessage req = MessageUtils.buildTaskListRequest();

            Rainstorm.ControlMessage resp = NetworkUtils.sendAndReceiveProto(m.host, 9000, req, 1200);

            if (resp == null || !resp.hasTaskList()) {
                System.out.println(m.host + " → no response");
                continue;
            }

            Rainstorm.TaskList tl = resp.getTaskList();

            for (Rainstorm.Task t : tl.getEntriesList()) {
                System.out.printf("VM=%s  taskId=%s  stage=%d  idx=%d  exe=%s  logfile=%s  pid=%s%n", m.nodeId, t.getId(), t.getStageValue(), t.getTaskIndex(), t.getOpExe(), t.getLogFile(), t.getPid());
            }
        }
    }

    public void stopAllTasks() {
        System.out.println("=== STOP_ALL_TASKS ===");

        for (Member m : node.getAliveNodes()) {
            Rainstorm.ControlMessage msg = MessageUtils.buildStopAllTasksMessage();
            NetworkUtils.sendProtoMessage(m.host, 9000, msg);
            System.out.println("Sent STOP_ALL_TASKS to VM " + m.host);
        }

        // stop report listener
        if (reportListener != null) {
            reportListener.stop();
            reportListener = null;
        }

        // stop autoscaler
        if (autoscaler != null) {
            autoscaler.stop();
            autoscaler = null;
        }

        topology.clear();
        routingMap.clear();
        stageTaskRates.clear();

        System.out.println("All RainStorm tasks stopped. Leader reset complete.");
    }


    public void killTask(String nodeId, int pid) {
        Optional<Member> member = node.getMember(nodeId);
        Rainstorm.ControlMessage msg = MessageUtils.buildKillTaskMessage(pid);
        if (member.isPresent()) {
            NetworkUtils.sendProtoMessage(member.get().host, 9000, msg);
            System.out.println("[Leader] KillTask sent to " + member.get().host + " for PID " + pid);
        } else {
            System.out.println("[Leader] Node: " + nodeId + " not found");
        }
    }

    private void cleanupTaskRate(int stage, int taskIndex) {
        Map<Integer, Double> taskRates = stageTaskRates.get(stage);
        if (taskRates != null) {
            taskRates.remove(taskIndex);
            // Optionally remove the stage entry if it becomes empty
            if (taskRates.isEmpty()) {
                stageTaskRates.remove(stage);
            }
        }
    }

    public boolean isStatefulStage(int stage) {
        if (stage < 1 || stage > numStages) return false;
        String opExe = opExecList.get(stage - 1);
        return "AggregateByKey".equals(opExe);
    }
}
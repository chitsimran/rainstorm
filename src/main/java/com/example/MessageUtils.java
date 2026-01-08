package com.example;

import com.example.rainstorm.TaskDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rainstorm.Rainstorm;

import java.util.List;

/**
 * Utility class for creating  messages.
 * Encapsulates construction of protobuf Membership.Message objects
 */
public class MessageUtils {
    private static Logger logger = LoggerFactory.getLogger(MessageUtils.class);

    private static Rainstorm.FileOpMessage logAndReturnFile(Rainstorm.FileOpMessage message) {
//        logger.info("Sending message with: {} bytes", message.getSerializedSize());
        return message;
    }

    public static Rainstorm.FileOpMessage localReadRequest(String dfsFile, String localFile) {
        return logAndReturnFile(Rainstorm.FileOpMessage.newBuilder()
                .build());
    }

    public static Rainstorm.FileOpMessage localAppendRequest(byte[] data, String dfsFile) {
        return logAndReturnFile(Rainstorm.FileOpMessage.newBuilder()
                .build());
    }

    public static Rainstorm.Message tupleMessage(Rainstorm.Tuple tuple) {
        return Rainstorm.Message.newBuilder()
                .setType(Rainstorm.Type.TUPLE)
                .setTuple(tuple)
                .build();
    }

    public static Rainstorm.ControlMessage logAndReturnControl(Rainstorm.ControlMessage message) {
//        logger.info("Sending ControlMessage with {} bytes", message.getSerializedSize());
        return message;
    }

    public static Rainstorm.ControlMessage buildStopTaskMessage(String taskId) {
        return logAndReturnControl(
                Rainstorm.ControlMessage.newBuilder()
                        .setStop(
                                Rainstorm.StopTask.newBuilder()
                                        .setTaskId(taskId)
                                        .build()
                        )
                        .build()
        );
    }

    public static Rainstorm.ControlMessage buildTaskEndedMessage(String taskId, int stage, String vmIp) {
        return logAndReturnControl(
                Rainstorm.ControlMessage.newBuilder()
                        .setEnded(
                                Rainstorm.TaskEnded.newBuilder()
                                        .setTaskId(taskId)
                                        .setStage(stage)
                                        .setVmIp(vmIp)
                                        .build()
                        )
                        .build()
        );
    }


    public static Rainstorm.ControlMessage buildControlPayloadMessage(
            List<Rainstorm.Metadata> metadataList,
            boolean autoscaleEnabled,
            Rainstorm.Type type,
            List<Rainstorm.Task> allTasksList) {
        return logAndReturnControl(
                Rainstorm.ControlMessage.newBuilder()
                        .setPayload(
                                Rainstorm.ControlPayload.newBuilder()
                                        .setType(type)
                                        .addAllMetadata(metadataList)
                                        .addAllAllTasks(allTasksList)
                                        .setAutoscale(autoscaleEnabled)
                                        .build()
                        )
                        .build()
        );
    }

    public static Rainstorm.ControlMessage buildLoadReportMessage(int stage, int taskIndex,
                                                                  double rate,
                                                                  String vmIp, String taskId) {
        return logAndReturnControl(
                Rainstorm.ControlMessage.newBuilder()
                        .setLoad(
                                Rainstorm.LoadReport.newBuilder()
                                        .setStage(stage)
                                        .setTaskIndex(taskIndex)
                                        .setRate(rate)
                                        .setVmIp(vmIp)
                                        .setTaskId(taskId)
                                        .build()
                        )
                        .build()
        );
    }

    public static Rainstorm.ControlMessage buildTaskListMessage(List<Rainstorm.Task> taskEntries) {

        Rainstorm.TaskList taskList = Rainstorm.TaskList.newBuilder()
                .addAllEntries(taskEntries)
                .build();

        return Rainstorm.ControlMessage.newBuilder()
                .setTaskList(taskList)
                .build();
    }

    public static Rainstorm.ControlMessage buildFailureReportMessage(
            String taskId,
            int stage
    ) {
        return logAndReturnControl(
                Rainstorm.ControlMessage.newBuilder()
                        .setFail(
                                Rainstorm.FailureReport.newBuilder()
                                        .setTaskId(taskId)
                                        .setStage(stage)
                                        .build()
                        )
                        .build()
        );
    }

    public static Rainstorm.Message buildStopMessage() {
        return Rainstorm.Message.newBuilder()
                .setType(Rainstorm.Type.STOP)
                .build();
    }

    public static Rainstorm.ControlMessage buildTaskAckMessage(String taskId) {
        return logAndReturnControl(
                Rainstorm.ControlMessage.newBuilder()
                        .setAck(
                                Rainstorm.TaskAck.newBuilder()
                                        .setTaskId(taskId)
                                        .build()
                        )
                        .build()
        );
    }

    public static Rainstorm.Message buildTupleAckMessage(String tupleId) {
        return Rainstorm.Message.newBuilder()
                .setType(Rainstorm.Type.ACK)
                .setTuple(Rainstorm.Tuple.newBuilder()
                        .setId(tupleId).build())
                .build();
    }


    public static Rainstorm.Message buildTaskPingMessage(String taskId) {
        return Rainstorm.Message.newBuilder()
                .setType(Rainstorm.Type.PING)
                .setTaskId(taskId)
                .build();
    }

    public static Rainstorm.ControlMessage buildTaskListRequest() {
        return Rainstorm.ControlMessage.newBuilder()
                .setListReq(Rainstorm.TaskListRequest.newBuilder().build())
                .build();
    }

    public static Rainstorm.ControlMessage buildStopAllTasksMessage() {
        return Rainstorm.ControlMessage.newBuilder()
                .setStopAllTasks(Rainstorm.StopAllTasks.newBuilder().build())
                .build();
    }


    public static Rainstorm.ControlMessage buildKillTaskMessage(int pid) {
        return Rainstorm.ControlMessage.newBuilder()
                .setKillTask(
                        Rainstorm.KillTask.newBuilder()
                                .setPid(pid)
                                .build()
                )
                .build();
    }

    public static Rainstorm.Message buildEOFMessage() {
        return Rainstorm.Message.newBuilder()
                .setType(Rainstorm.Type.EOF)
                .build();
    }

    public static Rainstorm.Task convertToProto(TaskDescriptor td) {
        return Rainstorm.Task.newBuilder()
                .setId(td.taskId)
                .setIp(td.ip)
                .setPort(td.dataPort)
                .setStage(convertStage(td.stage))   // map int → enum
                .setOpExe(td.opExe)
                .setLogFile(td.logfile)
                .setTaskIndex(td.taskIndex)
                .setSourceFile(td.sourceFile != null ? td.sourceFile : "")
                .setOpArgs(td.opArgs)
                .setAddOpArgs(td.addOpArgs == null ? "" : td.addOpArgs)
                .setDestinationFile(td.destinationFile)
                .setInputRate(td.inputRate)
                .build();
    }

    private static Rainstorm.Stage convertStage(int stage) {
        switch (stage) {
            case 0:
                return Rainstorm.Stage.SOURCE;   // Stage 1
            case 1:
                return Rainstorm.Stage.FIRST;    // Stage 2
            case 2:
                return Rainstorm.Stage.SECOND;   // Stage 3
            case 3:
                return Rainstorm.Stage.THIRD;    // Stage 4
            default:
                return Rainstorm.Stage.SOURCE;   // or throw an error
        }
    }
}


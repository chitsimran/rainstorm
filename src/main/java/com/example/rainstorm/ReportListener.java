package com.example.rainstorm;

import rainstorm.Rainstorm;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Optional;

public class ReportListener implements Runnable {

    private final RainStormMaster master;
    private final int reportPort;

    private volatile boolean running = true;
    private ServerSocket serverSocket;     // <-- needed so master can close it

    public ReportListener(RainStormMaster master, int reportPort) {
        this.master = master;
        this.reportPort = reportPort;
    }

    @Override
    public void run() {
        System.out.println("[Leader] ReportListener running on port " + reportPort);
        try {
            serverSocket = new ServerSocket(reportPort);
            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    handleMessage(socket);
                } catch (Exception e) {
                    if (!running) {
                        System.out.println("[Leader] ReportListener stopped.");
                        break;
                    }
                    System.err.println("[Leader] Error accepting connection: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            if (running) {
                System.err.println("[Leader] Failed to start ReportListener:");
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        System.out.println("[Leader] Stopping ReportListener...");
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();      // <-- unbinds port 9100
            }
        } catch (Exception ignored) {
        }
    }

    private void handleMessage(Socket socket) {
        try {
            Rainstorm.ControlMessage msg = Rainstorm.ControlMessage.parseDelimitedFrom(socket.getInputStream());
            if (msg == null) {
                socket.close();
                return;
            }
            if (msg.hasLoad()) {
                Rainstorm.LoadReport lr = msg.getLoad();
                double rate = lr.getRate();
                String taskId = msg.getLoad().getTaskId();
                Optional<TaskDescriptor> task = Optional.ofNullable(master.topology.findTaskById(msg.getLoad().getStage(), taskId));
                task.ifPresent(taskDescriptor -> taskDescriptor.rate = rate);
            } else if (msg.hasFail()) {
                master.handleTaskFailure(msg.getFail());
            } else if (msg.hasEnded()) {
                master.sendStopTask(
                        msg.getEnded().getTaskId(),
                        msg.getEnded().getVmIp(),
                        msg.getEnded().getStage()
                );
            } else {
                System.err.println("[Leader] Unknown worker->leader message");
            }
            socket.close();
        } catch (Exception e) {
            System.err.println("[Leader] Error in ReportListener message handler:");
            e.printStackTrace();
        }
    }
}




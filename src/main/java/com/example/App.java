package com.example;

import com.example.rainstorm.Worker;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;

/**
 * Application entry point for running a node in the distributed failure detector.
 * <p>
 * - Reads environment variables for config
 * - Starts a Node with specified ID, port, and introducer settings.
 * - Interactive command-line interface for user commands.
 */
public class App {
    public static void main(String[] args) throws IOException, InterruptedException {
        String nodeId = Optional.ofNullable(System.getenv("NODE_ID"))
                .orElse("0");
        String port = Optional.ofNullable(System.getenv("PORT"))
                .orElse("6661");
        String isIntroducer = Optional.ofNullable(System.getenv("IS_INTRODUCER"))
                .orElse("false");
        String introducerAddress = "fa25-cs425-5510.cs.illinois.edu";
        String ip = InetAddress.getLocalHost().getHostAddress();

        //TODO Implement Node before running
        Node node = null;
        //node.start();

        if (!Boolean.parseBoolean(isIntroducer)) {
            Worker worker = new Worker(9000);
            worker.start();
        }

        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.print(">>> ");
            String input = sc.nextLine().trim();
            String[] userArgs = input.split("\\s+");
            if ("exit".equalsIgnoreCase(userArgs[0])) {
                node.exitNode();
                break;
            } else if ("rainstorm".equalsIgnoreCase(userArgs[0])) {
                if (!node.isIntroducer()) {
                    System.out.println("RainStorm can only be started from the introducer node.");
                    continue;
                }
                if (userArgs.length < 6) {
                    System.out.println("Usage: rainstorm <Nstages> <Ntasks> <op1_exe> <op1_args...> ... <hydfs_src> <hydfs_dest> <exactly_once> <autoscale> [INPUT_RATE LW HW]");
                    continue;
                }
                try {
                    node.startRainStorm(userArgs);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("RainStorm failed to start.");
                }
            } else if ("list_tasks".equalsIgnoreCase(userArgs[0])) {
                node.listRainstormTasks();
            }
            else if ("stop_all".equalsIgnoreCase(userArgs[0])){
                node.stopAllRainstormTasks();
            } else if ("kill_task".equalsIgnoreCase(userArgs[0])) {
                if (userArgs.length != 3) {
                    System.out.println("Usage: kill_task <vm_ip> <taskId>");
                    continue;
                }
                node.killRainstormTask(userArgs[1], userArgs[2]);
            }
        }
    }
}

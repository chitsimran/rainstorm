package com.example;

import java.util.List;
import java.util.Optional;

public interface Node {
    List<Member> getAliveNodes();

    Optional<Member> getMember(String nodeId);

    void start();

    void exitNode();

    boolean isIntroducer();

    void startRainStorm(String[] args);

    void listRainstormTasks();

    void stopAllRainstormTasks();

    void killRainstormTask(String ip, String taskId);
}

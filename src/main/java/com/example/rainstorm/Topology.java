package com.example.rainstorm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Represents the entire RainStorm application topology:
 * - A set of stages (1..N)
 * - Each stage has multiple parallel tasks
 * - Each task is described by a TaskDescriptor
 * <p>
 * This class makes task assignment, autoscaling, and failure recovery
 * much easier and cleaner than using raw maps.
 */
public class Topology {

    // stage → list of TaskDescriptors
    private final Map<Integer, List<TaskDescriptor>> stageTasks = new HashMap<>();

    /**
     * Initialize mapping for given number of stages.
     * Each stage starts with an empty task list.
     */
    public void initStages(int numStages) {
        for (int s = 0; s <= numStages; s++) {
            stageTasks.put(s, new ArrayList<>());
        }
    }

    public TaskDescriptor getSourceTask() {
        List<TaskDescriptor> list = stageTasks.get(0);
        return (list == null || list.isEmpty()) ? null : list.get(0);
    }

    /**
     * Add a task to a specific stage.
     */
    public void addTask(int stage, TaskDescriptor td) {
        stageTasks.get(stage).add(td);
    }

    public boolean removeLastTask(int stage) {
        List<TaskDescriptor> list = stageTasks.get(stage);
        if (list == null) return false;
        list.remove(list.size() - 1);
        return false;  // nothing removed
    }


    /**
     * Get tasks for a given stage.
     */
    public List<TaskDescriptor> getTasks(int stage) {
        return stageTasks.get(stage);
    }

    public void clear() {
        stageTasks.clear();
    }

    /**
     * Get number of tasks in a given stage.
     */
    public int getNumTasks(int stage) {
        return stageTasks.get(stage).size();
    }

    /**
     * A direct view of the internal map.
     */
    public Map<Integer, List<TaskDescriptor>> getStageMap() {
        return stageTasks;
    }

    /**
     * Useful for debugging topology assignments.
     */
    public void printTopology() {
        System.out.println("===== RainStorm Topology =====");
        for (int s : stageTasks.keySet()) {
            System.out.println("Stage " + s + ":");
            for (TaskDescriptor td : stageTasks.get(s)) {
                System.out.println("  - " + td);
            }
        }
    }

    public Map<String, List<TaskDescriptor>> groupTasksByVm() {
        Map<String, List<TaskDescriptor>> map = new HashMap<>();

        for (int stage : stageTasks.keySet()) {
            for (TaskDescriptor td : stageTasks.get(stage)) {
                map.computeIfAbsent(td.ip, k -> new ArrayList<>()).add(td);
            }
        }
        return map;
    }

    public boolean removeTaskById(int stage, String taskId) {
        List<TaskDescriptor> list = stageTasks.get(stage);
        if (list == null) return false;

        Iterator<TaskDescriptor> it = list.iterator();
        while (it.hasNext()) {
            TaskDescriptor td = it.next();
            if (td.taskId.equals(taskId)) {
                it.remove();
                return true;
            }
        }
        return false;
    }

    public TaskDescriptor findTaskById(int stage, String taskId) {
        List<TaskDescriptor> list = stageTasks.get(stage);
        if (list == null) return null;
        for (TaskDescriptor td : list) {
            if (td.taskId.equals(taskId)) {
                return td;
            }
        }
        return null;
    }

    public void replaceTask(int stage, int taskIndex, TaskDescriptor newTd) {
        List<TaskDescriptor> list = stageTasks.get(stage);
        if (list == null) return;
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).taskIndex == taskIndex) {
                list.set(i, newTd);
                return;
            }
        }
    }

}


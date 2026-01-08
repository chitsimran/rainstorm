package com.example.rainstorm;

import com.example.Member;

import java.util.Map;

public class AutoScaler implements Runnable {

    private final RainStormMaster master;
    private final int numStages;
    private final double LW;
    private final double HW;
    private volatile boolean running = true;

    public AutoScaler(RainStormMaster master, int numStages, double LW, double HW) {
        this.master = master;
        this.numStages = numStages;
        this.LW = LW;
        this.HW = HW;
    }

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        System.out.println("[AutoScaler] Running (LW=" + LW + ", HW=" + HW + ")");
        while (running) {
            try {
                // 1. Query avg input rate per stage
                Map<Integer, Double> loads = master.queryStageLoads();
                // 2. Decide per-stage action
                for (int stage = 1; stage <= numStages; stage++) {
                    if (master.isStatefulStage(stage)) {
                        continue;
                    }
                    double load = loads.getOrDefault(stage, 0.0);
                    if (load > HW) {
                        Member bestVM = master.getNextVM();
                        if (bestVM != null) {
                            master.scaleUp(stage, bestVM, load);
                            Thread.sleep(2000);
                        }
                    } else if (load < LW && load > 0 && master.topology.getNumTasks(stage) > 1) {
                        master.scaleDown(stage, load);
                        Thread.sleep(2000);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

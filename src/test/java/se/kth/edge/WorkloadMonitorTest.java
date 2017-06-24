package se.kth.edge;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Hooman on 2017-06-24.
 */
public class WorkloadMonitorTest {

    @Test
    public void computeWeights() {

        WorkloadMonitor monitor = new WorkloadMonitor();
        // Test default history size and default weights computation
        float[] weights = monitor.getWeights();
        assert weights.length == 1;
        assert weights[0] == 1;

        int historySize = 2;
        float beta = 0.25f;
        monitor = new WorkloadMonitor(historySize, beta);
        weights = monitor.getWeights();
        assert weights.length == historySize;
        float w1 = beta / (1 + beta);
        float w2 = (float) 1 / (1 + beta);
        assert weights[0] == w1;
        assert weights[1] == w2;

    }

    @Test
    public void addKeyArrival() throws Exception {
        WorkloadMonitor monitor = new WorkloadMonitor();
        monitor.addKeyArrival(0, 1);
        assert monitor.getnArrivals() == 1;
        monitor.addKeyArrival(0, 2);
        assert monitor.getnArrivals() == 2;
        monitor.addKeyArrival(1, 3);
        assert monitor.getnArrivals() == 3;
    }

    @Test
    public void endOfWindow() throws Exception {
        WorkloadMonitor monitor = new WorkloadMonitor();
        monitor.addKeyArrival(0, 1);
        monitor.addKeyArrival(1, 3);
        monitor.endOfWindow();
        assert monitor.getnArrivals() == 0;
    }
}
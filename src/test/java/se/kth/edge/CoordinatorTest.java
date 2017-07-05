package se.kth.edge;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Hooman on 2017-06-25.
 */
public class CoordinatorTest {
    @Test
    public void registerKeys() throws Exception {
        int numEdges = 3;
        Coordinator c = new Coordinator(numEdges);
        long k1 = 1;
        long k2 = 2;
        long k3 = 3;
        long[] keys = new long[]{k1, k2, k3};
        int[] arrivals = new int[]{1, 1, 1};
        c.registerKeys(0, keys, arrivals);
        long[] keys2 = new long[]{k1, k3};
        int[] arrivals2 = new int[]{1, 1};
        c.registerKeys(2, keys2, arrivals2);
        Map<Long, int[]> map = c.getNewKeyArrivals();
        assert map.get(k1)[0] == 1;
        assert map.get(k1)[1] == 0;
        assert map.get(k1)[2] == 1;
        assert map.get(k2)[0] == 1;
        assert map.get(k2)[1] == 0;
        assert map.get(k2)[2] == 0;
        assert map.get(k3)[0] == 1;
        assert map.get(k3)[1] == 0;
        assert map.get(k3)[2] == 1;
    }

    @Test
    public void applyUpdates() throws Exception {
        int numEdges = 3;
        Coordinator c = new Coordinator(numEdges);
        long k1 = 1;
        long k2 = 2;
        int[] k1Edges = new int[]{1, 0, 1};
        int[] k2Edges = new int[]{1, 0, 1};
        HashMap<Long, int[]> newArrivals = new HashMap<>();
        newArrivals.put(k1, k1Edges);
        newArrivals.put(k2, k2Edges);
        Map<Long, Integer> keyEdgeMap = c.applyUpdates(newArrivals, new HashMap<>());
        int e1 = 0;
        int e2 = 1;
        int e3 = 2;
        int c1 = 0;
        int c2 = 0;
        int c3 = 0;
        for (int e : keyEdgeMap.values()) {
            if (e == e1) {
                c1++;
            } else if (e == e2) {
                c2++;
            } else if (e == e3) {
                c3++;
            }
        }
        assert c1 == 1;
        assert c2 == 0;
        assert c3 == 1;

        long k3 = 3;
        k1Edges = new int[]{0, 1, 0};
        k2Edges = new int[]{0, 1, 0};
        int[] k3Edges = new int[]{1, 1, 1};
        newArrivals = new HashMap<>();
        newArrivals.put(k1, k1Edges);
        newArrivals.put(k2, k2Edges);
        newArrivals.put(k3, k3Edges);
        keyEdgeMap = c.applyUpdates(newArrivals, new HashMap<>());
        c1 = 0;
        c2 = 0;
        c3 = 0;
        for (int e : keyEdgeMap.values()) {
            if (e == e1) {
                c1++;
            } else if (e == e2) {
                c2++;
            } else if (e == e3) {
                c3++;
            }
        }
        assert c1 == 0;
        assert c2 == 1;
        assert c3 == 0;

        HashMap<Integer, long[]> removals = new HashMap<>();
        removals.put(e1, new long[]{k1, k2});
        removals.put(e3, new long[]{k1, k2});
        keyEdgeMap = c.applyUpdates(new HashMap<>(), removals);
        c1 = 0;
        c2 = 0;
        c3 = 0;
        for (int e : keyEdgeMap.values()) {
            if (e == e1) {
                c1++;
            } else if (e == e2) {
                c2++;
            } else if (e == e3) {
                c3++;
            }
        }
        assert c1 == 0;
        assert c2 == 2;
        assert c3 == 0;

        removals = new HashMap<>();
        removals.put(e2, new long[]{k1});
        keyEdgeMap = c.applyUpdates(new HashMap<>(), removals);
        assert keyEdgeMap.get(k1) == -1;


        // MAX Arrival Strategy
        c = new Coordinator(numEdges, Coordinator.SelectionStrategy.MAX_ARRIVAL);
        k1Edges = new int[]{2, 0, 1};
        k2Edges = new int[]{1, 0, 2};
        newArrivals = new HashMap<>();
        newArrivals.put(k1, k1Edges);
        newArrivals.put(k2, k2Edges);
        keyEdgeMap = c.applyUpdates(newArrivals, new HashMap<>());
        c1 = 0;
        c2 = 0;
        c3 = 0;
        for (int e : keyEdgeMap.values()) {
            if (e == e1) {
                c1++;
            } else if (e == e2) {
                c2++;
            } else if (e == e3) {
                c3++;
            }
        }
        assert c1 == 1;
        assert c2 == 0;
        assert c3 == 1;
        assert keyEdgeMap.get(k1) == e1;
        assert keyEdgeMap.get(k2) == e3;
        k1Edges = new int[]{0, 3, 0};
        newArrivals = new HashMap<>();
        newArrivals.put(k1, k1Edges);
        keyEdgeMap = c.applyUpdates(newArrivals, new HashMap<>());
        assert keyEdgeMap.get(k1) == e2;
    }

}
package se.kth.edge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Hooman on 2017-06-24.
 */
public class Coordinator {

    private final int numEdges;
    private final HashMap<Long, int[]> keyEdgeArrivals = new HashMap<>();
    private final HashMap<Long, Integer> keyCoordinators = new HashMap<>();
    private final int[] edgeKeyCounters;
    private final HashMap<Long, int[]> newKeyArrivals = new HashMap<>();
    private final HashMap<Integer, long[]> newKeyRemovals = new HashMap<>();

    public Coordinator(int numEdges) {
        this.numEdges = numEdges;
        this.edgeKeyCounters = new int[numEdges];
    }

    public void registerKeys(int edgeId, long[] keys, int[] arrivals) throws Exception {
        if (keys.length != arrivals.length) {
            throw new Exception("Number of keys does not match number of arrivals.");
        }
        for (int i = 0; i < keys.length; i++) {
            int[] arrivalsPerEdge;
            long k = keys[i];
            if (getNewKeyArrivals().containsKey(k)) {
                arrivalsPerEdge = getNewKeyArrivals().get(k);
            } else {
                arrivalsPerEdge = new int[numEdges];
                getNewKeyArrivals().put(k, arrivalsPerEdge);
            }

            arrivalsPerEdge[edgeId] = arrivals[i];
        }
    }

    public void unregisterKeys(int edgeId, long[] keys) {
        getNewKeyRemovals().put(edgeId, keys);
    }

    public Map<Long, Integer> computeNewCoordinators() throws Exception {
        Map<Long, Integer> keyEdgeMap = applyUpdates(getNewKeyArrivals(), getNewKeyRemovals());
        getNewKeyArrivals().clear();
        getNewKeyRemovals().clear();
        return keyEdgeMap;
    }

    public Map<Long, Integer> applyUpdates(HashMap<Long, int[]> newKeyArrivals, HashMap<Integer, long[]>
            newKeyRemovals) throws Exception {
        Map<Long, Integer> keyEdgeMap = new HashMap<>();
        Set<Long> keysToUpdate = new HashSet<>();

        for (int e : newKeyRemovals.keySet()) {
            long[] keys = newKeyRemovals.get(e);
            for (long k : keys) {
                int[] edges = keyEdgeArrivals.get(k);
                if (edges == null || edges[e] == 0)
                    throw new Exception("Inconsistent state for the registered keys.");
                edges[e] = 0;
                if (keyCoordinators.containsKey(k) && keyCoordinators.get(k) == e) {
                    keysToUpdate.add(k);
                    keyCoordinators.remove(k);
                    edgeKeyCounters[e] -= 1;
                }
            }
        }

        for (long k : newKeyArrivals.keySet()) {
            keysToUpdate.add(k);
            int[] newArrivalsPerEdge = newKeyArrivals.get(k);
            int[] currentArrivalsPerEdge = keyEdgeArrivals.get(k);
            if (currentArrivalsPerEdge == null) {
                currentArrivalsPerEdge = new int[numEdges];
                keyEdgeArrivals.put(k, currentArrivalsPerEdge);
            }
            for (int i = 0; i < numEdges; i++) {
                if (newArrivalsPerEdge[i] > 0) {
                    if (currentArrivalsPerEdge[i] == 0) {
                        currentArrivalsPerEdge[i] = newArrivalsPerEdge[i];
                    } else {
                        throw new Exception("Inconsistent state for the registered keys.");
                    }
                }
            }
        }

        // update the selected keys
        for (long k : keysToUpdate) {
            int[] arrivalsPerEdge = keyEdgeArrivals.get(k);
            // Choose the edge with the minimum keys assigned before (load balancing)
            int minCoordinator = -1;
            int currentCoordinator = -1;
            int minLoad = Integer.MAX_VALUE;
            if (keyCoordinators.containsKey(k)) {
                minCoordinator = keyCoordinators.get(k);
                currentCoordinator = minCoordinator;
                minLoad = edgeKeyCounters[minCoordinator];
            }
            for (int i = 0; i < numEdges; i++) {
                if (arrivalsPerEdge[i] > 0) {
                    if ((edgeKeyCounters[i] < minLoad && currentCoordinator == -1) || (edgeKeyCounters[i] + 1 <
                            minLoad && currentCoordinator != -1)) {
                        minLoad = edgeKeyCounters[i];
                        minCoordinator = i;
                    }
                }
            }
            if (minCoordinator == -1 && minCoordinator != currentCoordinator) {
                throw new Exception("Inconsistent state for the registered keys.");
            }

            if (minCoordinator == -1)
            {
                keyEdgeMap.put(k, -1); // No coordinator can be assigned. Edges go for oblivious approach!
                // You can remove the key from the keyEdgeArrivals
            } else if (minCoordinator != currentCoordinator) {
                if (keyCoordinators.containsKey(k)) {
                    int e = keyCoordinators.get(k);
                    edgeKeyCounters[e] -= 1;
                }
                keyEdgeMap.put(k, minCoordinator);
                edgeKeyCounters[minCoordinator] += 1;
                keyCoordinators.put(k, minCoordinator);
            }

        }

        return keyEdgeMap;
    }

    public int getNumEdges() {
        return numEdges;
    }

    public HashMap<Long, int[]> getNewKeyArrivals() {
        return newKeyArrivals;
    }

    public HashMap<Integer, long[]> getNewKeyRemovals() {
        return newKeyRemovals;
    }
}

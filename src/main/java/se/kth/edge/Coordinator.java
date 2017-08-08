package se.kth.edge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Hooman on 2017-06-24.
 */
public class Coordinator {

    public final static int SELF = -1;
    private final int numEdges;
    private final HashMap<Long, float[]> keyEdgeArrivals = new HashMap<>();
    private final HashMap<Long, Integer> keyCoordinators = new HashMap<>();
    private final int[] edgeKeyCounters;
    private final HashMap<Long, float[]> newKeyArrivals = new HashMap<>();
    private final HashMap<Integer, long[]> newKeyRemovals = new HashMap<>();
    private final SelectionStrategy selectionStrategy;
    public static final SelectionStrategy DEFAULT_SELECTION_STRATEGY = SelectionStrategy.MIN_LOAD;
    private int regCounter;
    private int longMessages;
    private int intMessages;
    private int remCounter;

    public Coordinator(int numEdges) {
        this(numEdges, DEFAULT_SELECTION_STRATEGY);
    }

    public Coordinator(int numEdges, SelectionStrategy ss) {
        this.numEdges = numEdges;
        this.edgeKeyCounters = new int[numEdges];
        this.selectionStrategy = ss;
    }

    public void registerKeys(int edgeId, long[] keys, float[] arrivals) throws Exception {
        regCounter++;
        longMessages += keys.length;
        intMessages += arrivals.length + 1;

        if (keys.length != arrivals.length) {
            throw new Exception("Number of keys does not match number of arrivals.");
        }
        for (int i = 0; i < keys.length; i++) {
            float[] arrivalsPerEdge;
            long k = keys[i];
            if (getNewKeyArrivals().containsKey(k)) {
                arrivalsPerEdge = getNewKeyArrivals().get(k);
            } else {
                arrivalsPerEdge = new float[numEdges];
                getNewKeyArrivals().put(k, arrivalsPerEdge);
            }

            arrivalsPerEdge[edgeId] = arrivals[i];
        }
    }

    public void unregisterKeys(int edgeId, long[] keys) {
        remCounter++;
        longMessages += keys.length;
//        intMessages++; // Only count for register and not removal.

        getNewKeyRemovals().put(edgeId, keys);
    }

    public Map<Long, Integer> computeNewCoordinators() throws Exception {
        Map<Long, Integer> keyEdgeMap = applyUpdates(getNewKeyArrivals(), getNewKeyRemovals());
        getNewKeyArrivals().clear();
        getNewKeyRemovals().clear();
        return keyEdgeMap;
    }

    public void resetStatistics() {
        remCounter = 0;
        regCounter = 0;
        longMessages = 0;
        intMessages = 0;
    }

    public Map<Long, Integer> applyUpdates(HashMap<Long, float[]> newKeyArrivals, HashMap<Integer, long[]>
            newKeyRemovals) throws Exception {
        Set<Long> keysToUpdate = new HashSet<>();

        for (int e : newKeyRemovals.keySet()) {
            long[] keys = newKeyRemovals.get(e);
            for (long k : keys) {
                float[] edges = keyEdgeArrivals.get(k);
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
            float[] newArrivalsPerEdge = newKeyArrivals.get(k);
            float[] currentArrivalsPerEdge = keyEdgeArrivals.get(k);
            if (currentArrivalsPerEdge == null) {
                currentArrivalsPerEdge = new float[numEdges];
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
        Map<Long, Integer> keyEdgeMap;
        if (selectionStrategy == SelectionStrategy.MIN_LOAD) {
            keyEdgeMap = assignCoordinatorsWithMinLoad(keysToUpdate);
        } else {
            keyEdgeMap = assignCoordinatorsWithMaxArrival(keysToUpdate);
        }

        return keyEdgeMap;
    }

    private Map<Long, Integer> assignCoordinatorsWithMaxArrival(Set<Long> keysToUpdate) throws Exception {
        Map<Long, Integer> keyEdgeMap = new HashMap<>();
        for (long k : keysToUpdate) {
            float[] arrivalsPerEdge = keyEdgeArrivals.get(k);
            int maxCoordinator = SELF;
            int currentCoordinator = SELF;
            float maxArrival = Float.MIN_VALUE;
            if (keyCoordinators.containsKey(k)) {
                maxCoordinator = keyCoordinators.get(k);
                currentCoordinator = maxCoordinator;
                maxArrival = arrivalsPerEdge[maxCoordinator];
            }
            for (int i = 0; i < numEdges; i++) {
                if (arrivalsPerEdge[i] > 0) {
                    if (arrivalsPerEdge[i] > maxArrival) {
                        maxArrival = arrivalsPerEdge[i];
                        maxCoordinator = i;
                    }
                }
            }
            if (maxCoordinator == SELF && maxCoordinator != currentCoordinator) {
                throw new Exception("Inconsistent state for the registered keys.");
            }

            if (maxCoordinator == SELF) {
                keyEdgeMap.put(k, SELF); // No coordinator can be assigned. Edges go for oblivious approach!
                // You can remove the key from the keyEdgeArrivals
            } else if (maxCoordinator != currentCoordinator) {
                if (keyCoordinators.containsKey(k)) {
                    int e = keyCoordinators.get(k);
                    edgeKeyCounters[e] -= 1;
                }
                keyEdgeMap.put(k, maxCoordinator);
                edgeKeyCounters[maxCoordinator] += 1;
                keyCoordinators.put(k, maxCoordinator);
            }
        }

        return keyEdgeMap;
    }

    private Map<Long, Integer> assignCoordinatorsWithMinLoad(Set<Long> keysToUpdate) throws Exception {
        Map<Long, Integer> keyEdgeMap = new HashMap<>();
        for (long k : keysToUpdate) {
            float[] arrivalsPerEdge = keyEdgeArrivals.get(k);
            // Choose the edge with the minimum keys assigned before (load balancing)
            int minCoordinator = SELF;
            int currentCoordinator = SELF;
            int minLoad = Integer.MAX_VALUE;
            if (keyCoordinators.containsKey(k)) {
                minCoordinator = keyCoordinators.get(k);
                currentCoordinator = minCoordinator;
                minLoad = edgeKeyCounters[minCoordinator];
            }
            for (int i = 0; i < numEdges; i++) {
                if (arrivalsPerEdge[i] > 0) {
                    if ((edgeKeyCounters[i] < minLoad && currentCoordinator == SELF) || (edgeKeyCounters[i] + 1 <
                            minLoad && currentCoordinator != SELF)) {
                        minLoad = edgeKeyCounters[i];
                        minCoordinator = i;
                    }
                }
            }
            if (minCoordinator == SELF && minCoordinator != currentCoordinator) {
                throw new Exception("Inconsistent state for the registered keys.");
            }

            if (minCoordinator == SELF) {
                keyEdgeMap.put(k, SELF); // No coordinator can be assigned. Edges go for oblivious approach!
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

    public HashMap<Long, float[]> getNewKeyArrivals() {
        return newKeyArrivals;
    }

    public HashMap<Integer, long[]> getNewKeyRemovals() {
        return newKeyRemovals;
    }

    public SelectionStrategy getSelectionStrategy() {
        return selectionStrategy;
    }

    public int getRegCounter() {
        return regCounter;
    }

    public int getLongMessages() {
        return longMessages;
    }

    public int getIntMessages() {
        return intMessages;
    }

    public int getRemCounter() {
        return remCounter;
    }

    public int[] getEdgeKeyCounters() {
        return edgeKeyCounters;
    }

    public enum SelectionStrategy {
        MIN_LOAD, MAX_ARRIVAL
    }
}

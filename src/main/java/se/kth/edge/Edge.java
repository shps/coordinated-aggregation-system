package se.kth.edge;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by Hooman on 2017-06-24.
 */
public class Edge {

    private final int eId;
    private final WorkloadMonitor wMonitor;
    private final CacheManager cache;
    private final Map<Long, Integer> coordinators = new HashMap<>();
    private int coordinatorMessages;
    private int longMessages;
    private int intMessages;

    public Edge(int eId, CacheManager c, WorkloadMonitor m) {
        this.eId = eId;
        this.cache = c;
        this.wMonitor = m;
    }

    public void keyArrival(long kid, long time) {
        this.keyArrival(kid, time, this.eId);
    }

    public void keyArrival(long kid, long time, int edgeId) {
        if (edgeId != eId) {
            // TODO this is an update from other edges
        }
        if (coordinators.containsKey(kid) && coordinators.get(kid) == eId) {
            cache.insertPriorityKey(kid, time); // prioritize keys that the edge is its coordinator.
        } else {
            cache.insert(kid, time);
        }
        wMonitor.addKeyArrival(kid, time);
    }

    public long[] trigger(long t, long windowStartTime, float avgBw) {
        long[] updates = cache.trigger(t, windowStartTime, avgBw, wMonitor.getnArrivals(), wMonitor
                .getArrivalsHistories());
        return updates;
    }

    public Map<Integer, List<Long>> getKeyCoordinator(long[] keys) {
        Map<Integer, List<Long>> kc = new HashMap<>();
        for (long key : keys) {
            Integer c = coordinators.get(key);
            if (c == null || c == Coordinator.SELF) {
                c = eId;
            }

            List<Long> edgeKeys = kc.get(c);
            if (edgeKeys == null) {
                edgeKeys = new LinkedList<>();
                kc.put(c, edgeKeys);
            }

            edgeKeys.add(key);
        }

        return kc;
    }

    public void updateCoordinators(Map<Long, Integer> newCoordinators) {
        coordinatorMessages++;
        longMessages += newCoordinators.keySet().size();
        intMessages += newCoordinators.values().size();
        this.getCoordinators().putAll(newCoordinators);
    }

    public long[] endOfWindow() throws Exception {
        long[] remainingKeys = cache.endOfWindow();
        wMonitor.endOfWindow();

        return remainingKeys;
    }

    public void resetStatistics() {
        coordinatorMessages = 0;
        longMessages = 0;
        intMessages = 0;
    }

    public WorkloadMonitor getWorkloadManager() {
        return wMonitor;
    }

    public int getId() {
        return eId;
    }

    public CacheManager getCacheManager() {
        return cache;
    }

    public Map<Long, Integer> getCoordinators() {
        return coordinators;
    }

    public int getCoordinatorMessages() {
        return coordinatorMessages;
    }

    public int getLongMessages() {
        return longMessages;
    }

    public int getIntMessages() {
        return intMessages;
    }
}

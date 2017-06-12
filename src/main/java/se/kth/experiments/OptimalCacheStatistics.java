package se.kth.experiments;

import se.kth.stream.Tuple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Hooman on 2017-06-11.
 */
public class OptimalCacheStatistics {

    private final List<Long> triggerTimes = new LinkedList<>();
    private final List<List<Long>> triggeredKeys = new LinkedList<>();
    private final LinkedList<Integer> cacheSizes = new LinkedList();
    private final LinkedList<Integer> updateSizes = new LinkedList();

    public OptimalCacheStatistics(LinkedList<Tuple> tuplesPerWindow) {
        computeOptimalCacheSizes(tuplesPerWindow);
    }

    public OptimalCacheStatistics(LinkedList<Tuple> tuplesPerWindow, long time, int timestep, int window) {
        computeOptimalCacheSizes(tuplesPerWindow, time, timestep, window);
    }

    private List<Integer> computeOptimalCacheSizes(LinkedList<Tuple> tuplesPerWindow) {

        HashMap<Long, Long> keyFutureArrivals = new HashMap<>();
        for (Tuple t : tuplesPerWindow) {
            if (!keyFutureArrivals.containsKey(t.getKey())) {
                keyFutureArrivals.put(t.getKey(), 0L);
            }

            keyFutureArrivals.put(t.getKey(), keyFutureArrivals.get(t.getKey()) + 1);
        }

        int leftKeys = keyFutureArrivals.size();
        HashSet<Long> cachedKeys = new HashSet<>();

        for (Tuple t : tuplesPerWindow) {
            cachedKeys.add(t.getKey());
            long n = keyFutureArrivals.get(t.getKey());
            n = n - 1;
            keyFutureArrivals.put(t.getKey(), n);
            int updateSize = 0;
            if (n == 0) {
                cachedKeys.remove(t.getKey());
                getTriggerTimes().add(t.getTimestamp()); //TODO trigger can be based on time steps.
                List<Long> keys = new LinkedList<>();
                keys.add(t.getKey());
                getTriggeredKeys().add(keys);
                updateSize = 1;
            }
            getUpdateSizes().add(updateSize);
            getCacheSizes().add(cachedKeys.size());
        }

        return getCacheSizes();
    }

    private List<Integer> computeOptimalCacheSizes(LinkedList<Tuple> tuples, long time, int timestep, int window) {

        HashMap<Long, Long> keyFutureArrivals = new HashMap<>();
        for (Tuple t : tuples) {
            if (!keyFutureArrivals.containsKey(t.getKey())) {
                keyFutureArrivals.put(t.getKey(), 0L);
            }

            keyFutureArrivals.put(t.getKey(), keyFutureArrivals.get(t.getKey()) + 1);
        }

        int leftKeys = keyFutureArrivals.size();
        HashSet<Long> cachedKeys = new HashSet<>();

        boolean windowStarts = true;
        int updateSize = 0;
        int cacheSize = 0;
        long currentTime = time;
        while (true) {
            if (windowStarts) {
                updateSize = 0;
                windowStarts = false;
            }
            getTriggerTimes().add(currentTime);
            getUpdateSizes().add(updateSize);
            getCacheSizes().add(cachedKeys.size());

            if (tuples.isEmpty() && currentTime == (time + window)) {
                break;
            }

            currentTime += timestep;

            while (tuples.peek() != null && (tuples.peek().getTimestamp() < currentTime)) {
                Tuple t = tuples.poll();
                cachedKeys.add(t.getKey());
                long n = keyFutureArrivals.get(t.getKey());
                n = n - 1;
                keyFutureArrivals.put(t.getKey(), n);
                if (n == 0) {
                    cachedKeys.remove(t.getKey());
                    getTriggerTimes().add(t.getTimestamp()); //TODO trigger can be based on time steps.
                    List<Long> keys = new LinkedList<>();
                    keys.add(t.getKey());
                    getTriggeredKeys().add(keys);
                    updateSize++;
                }
            }
        }

        return getCacheSizes();
    }

    public List<Long> getTriggerTimes() {
        return triggerTimes; // it should be a deep clone.
    }

    public List<List<Long>> getTriggeredKeys() {
        return triggeredKeys;
    }

    public LinkedList<Integer> getCacheSizes() {
        return cacheSizes;
    }

    public LinkedList<Integer> getUpdateSizes() {
        return updateSizes;
    }
}

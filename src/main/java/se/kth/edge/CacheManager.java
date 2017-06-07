package se.kth.edge;

import java.util.*;

/**
 * Created by Hooman on 2017-06-05.
 */
public class CacheManager {

    private HashMap<Integer, CacheEntry> cacheKeys = new HashMap<>();
    private PriorityQueue<CacheEntry> cache;
    private Map<Integer, List<Long>> keyArrivals = new HashMap<>();
    private Map<Integer, List<Long>> keyArrivalsPrevWindow = new HashMap<>();
    private int nArrivals = 0;
    private int nArrivalsPrevWindow = 0;
    private double laziness = 0.25;
    private int w;

    public enum EvictionPolicies {
        LRU, LFU
    }

    public CacheManager(EvictionPolicies policy, int window) {
        if (policy == EvictionPolicies.LRU) {
            cache = new PriorityQueue<>(new RecentlyUsedComparator());
        } else {
            cache = new PriorityQueue<>(new FrequentlyUsedComparator());
        }

        this.w = window;
    }

    public CacheManager(EvictionPolicies policy, int window, float laziness) {
        this(policy, window);
        this.laziness = laziness;
    }

    /**
     * cache insert.
     */
    public void insert(int kid, long time) {
        CacheEntry entry = null;
        if (cacheKeys.containsKey(kid)) {
            entry = cacheKeys.get(kid);
            cache.remove(entry);
            entry.numArrivals++;
        } else {
            entry = new CacheEntry();
            entry.numArrivals = 1;
            cacheKeys.put(kid, entry);
        }
        entry.lastUpdateTime = time;
        cache.add(entry);

        addKeyArrival(kid, time);
    }

    /**
     * @param kid
     * @param time arrival time
     */
    public void addKeyArrival(int kid, long time) {
        List<Long> arrivalTimes;
        if (keyArrivals.containsKey(kid)) {
            arrivalTimes = keyArrivals.get(kid);
        } else {
            arrivalTimes = new LinkedList<Long>();
            keyArrivals.put(kid, arrivalTimes);
        }

        arrivalTimes.add(time);
        nArrivals++;
    }

    public List<CacheEntry> trigger(long t, long windowStartTime, float avgBw) {
        List<CacheEntry> evictedEntries = new LinkedList<>();
        int size = computeCacheSize(t, windowStartTime, avgBw);

        if (cache.size() > size) {
            int evictionSize = cache.size() - size;
            for (int i = 0; i < evictionSize; i++) {
                evictedEntries.add(cache.poll());
            }
        }

        return evictedEntries;
    }

    // TODO: 2017-06-06  compute avgBw
    public int computeCacheSize(long t, long startTime, float avgBw) {
        int eagerSize = CacheSizePolicies.computeEagerOptimalOnline(t, startTime, w, keyArrivalsPrevWindow.values());
        double avgArrivalRate = 0;
        if (t != startTime) {
            avgArrivalRate = (double) nArrivals / (t - startTime);
        }
        int lazySize = CacheSizePolicies.computeLazyOptimalOnline(t, startTime + w, avgArrivalRate, avgBw);
        int cacheSize = (int) (laziness * lazySize + (1 - laziness) * eagerSize);
        return cacheSize;
    }

    /**
     * Call this method upon the start of each window.
     */
    public void nextWindow() {
        keyArrivalsPrevWindow = keyArrivals;
        keyArrivals = new HashMap<>();
        nArrivalsPrevWindow = nArrivals;
        nArrivals = 0;
        cacheKeys.clear();
        cache.clear();
    }

    
    private class FrequentlyUsedComparator implements Comparator<CacheEntry> {

        public int compare(CacheEntry e1, CacheEntry e2) {
            if (e1.numArrivals < e2.numArrivals) {
                return -1;
            }

            if (e1.numArrivals > e2.numArrivals) {
                return 1;
            }

            return 0;
        }
    }

    private class RecentlyUsedComparator implements Comparator<CacheEntry> {

        public int compare(CacheEntry e1, CacheEntry e2) {
            if (e1.lastUpdateTime < e2.lastUpdateTime) {
                return -1;
            }

            if (e1.lastUpdateTime > e2.lastUpdateTime) {
                return 1;
            }

            return 0;
        }
    }
}

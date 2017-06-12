package se.kth.edge;

import java.util.*;

/**
 * Created by Hooman on 2017-06-05.
 */
public class CacheManager {

    private HashMap<Long, CacheEntry> cacheKeys = new HashMap<>();
    private PriorityQueue<CacheEntry> cache;
    private Map<Long, List<Long>> keyArrivals = new HashMap<>();
    private Map<Long, List<Long>> keyArrivalsPrevWindow = new HashMap<>();
    private int nArrivals = 0;
    private int nArrivalsPrevWindow = 0;
    private double laziness = 0.25;
    private int w;
    private SizePolicy sPolicy;

    public int getnArrivals() {
        return nArrivals;
    }

    public enum EvictionPolicy {
        LRU, LFU
    }

    public enum SizePolicy {
        EAGER, LAZY, HYBRID
    }

    /**
     * @param window
     * @param sPolicy
     * @param ePolicy
     */
    public CacheManager(int window, SizePolicy sPolicy, EvictionPolicy ePolicy) {
        if (ePolicy == EvictionPolicy.LRU) {
            cache = new PriorityQueue<>(new RecentlyUsedComparator());
        } else {
            cache = new PriorityQueue<>(new FrequentlyUsedComparator());
        }
        this.sPolicy = sPolicy;
        this.w = window;
    }

    /**
     * @param window
     * @param s
     * @param e
     * @param laziness
     */
    public CacheManager(int window, SizePolicy s, EvictionPolicy e, float laziness) {
        this(window, s, e);
        this.laziness = laziness;
    }

    /**
     * @param kid
     * @param time
     */
    public void insert(long kid, long time) {
        CacheEntry entry = null;
        if (cacheKeys.containsKey(kid)) {
            entry = cacheKeys.get(kid);
            cache.remove(entry);
            entry.numArrivals++;
        } else {
            entry = new CacheEntry(kid);
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
    public void addKeyArrival(long kid, long time) {
        List<Long> arrivalTimes;
        if (keyArrivals.containsKey(kid)) {
            arrivalTimes = keyArrivals.get(kid);
        } else {
            arrivalTimes = new LinkedList<Long>();
            keyArrivals.put(kid, arrivalTimes);
        }

        arrivalTimes.add(time);
        nArrivals = getnArrivals() + 1;
    }

    /**
     * @param t
     * @param windowStartTime
     * @param avgBw
     * @return
     */
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


    /**
     * @param t
     * @param startTime
     * @param avgBw
     * @return
     */
    public int computeCacheSize(long t, long startTime, float avgBw) {
        int cacheSize = 0;
        if (sPolicy == SizePolicy.HYBRID) {
            int eagerSize = this.computeEagerOptimal(t, startTime);
            int lazySize = this.computeLazyOptimal(t, startTime, avgBw);
            cacheSize = (int) (laziness * lazySize + (1 - laziness) * eagerSize);
        } else if (sPolicy == SizePolicy.EAGER) {
            cacheSize = this.computeEagerOptimal(t, startTime);
        } else {
            cacheSize = this.computeLazyOptimal(t, startTime, avgBw);
        }
        return cacheSize;
    }

    public int computeEagerOptimal(long t, long startTime) {
        return CacheSizePolicies.computeEagerOptimalOnline(t, startTime, w, keyArrivalsPrevWindow.values());
    }

    public int computeLazyOptimal(long t, long startTime, float avgBw) {
        double avgArrivalRate = 0;
        if (t != startTime) {
            avgArrivalRate = (double) getnArrivals() / (t - startTime);
        }
        return CacheSizePolicies.computeLazyOptimalOnline(t, startTime + w, avgArrivalRate, avgBw);
    }

    /**
     * Call this method upon the start of each window.
     */
    public void nextWindow() {
        keyArrivalsPrevWindow = keyArrivals;
        keyArrivals = new HashMap<>();
        nArrivalsPrevWindow = getnArrivals();
        nArrivals = 0;
        cacheKeys.clear();
        cache.clear();
    }

    public int getCurrentCacheSize() {
        return cache.size();
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

package se.kth.edge;

import java.util.*;

/**
 * Created by Hooman on 2017-06-05.
 */
public class CacheManager {

    private final HashMap<Long, CacheEntry> cacheKeys = new HashMap<>();
    private final PriorityQueue<CacheEntry> cache;
    private final Set<Long> keyHasArrived = new HashSet<>();
    private final Map<Long, Key> arrivalsHistories = new HashMap<>();
    private final float[] weights;
    private int nArrivals = 0;
    private int nArrivalsPrevWindow = 0;
    private final float laziness;
    private int w;
    private final SizePolicy sPolicy;
    private final float beta;
    public final static float DEFAULT_BETA = 0.5f;
    public final static int DEFAULT_HISTORY_SIZE = 1;
    public final static float DEFAULT_LAZINESS = 0.25f;

    public int getnArrivals() {
        return nArrivals;
    }

    public float[] getWeights() {
        return weights;
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

        this(window, sPolicy, ePolicy, DEFAULT_LAZINESS);
    }

    /**
     * @param window
     * @param s
     * @param e
     * @param laziness
     */
    public CacheManager(int window, SizePolicy s, EvictionPolicy e, float laziness) {

        this(window, s, e, laziness, DEFAULT_HISTORY_SIZE, DEFAULT_BETA);
    }

    /**
     * @param window
     * @param s
     * @param e
     * @param laziness
     */
    public CacheManager(int window, SizePolicy s, EvictionPolicy e, float laziness, int historySize, float beta) {

        if (e == EvictionPolicy.LRU) {
            cache = new PriorityQueue<>(new RecentlyUsedComparator());
        } else {
            cache = new PriorityQueue<>(new FrequentlyUsedComparator());
        }
        this.sPolicy = s;
        this.w = window;
        this.laziness = laziness;
        this.beta = beta;
        weights = computeWeights(historySize, beta);
    }

    private float[] computeWeights(int historySize, float beta) {
        float[] weights = new float[historySize];
        int size = weights.length;
        float[] wPrime = new float[size];
        float sum = 0;
        for (int i = 0; i < size; i++) {
            wPrime[i] = (float) Math.pow(beta, size - i + 1);
            sum += wPrime[i];
        }

        for (int i = 0; i < size; i++) {
            weights[i] = wPrime[i] / sum;
        }

        return weights;
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
        Key k;
        if (arrivalsHistories.containsKey(kid)) {
            k = arrivalsHistories.get(kid);
        } else {
            k = new Key(kid, getWeights());
            arrivalsHistories.put(kid, k);
        }
        k.increaseArrival();
        nArrivals = getnArrivals() + 1;
        keyHasArrived.add(kid);
    }

    /**
     * @param t
     * @param windowStartTime
     * @param avgBw
     * @return
     */
    public long[] trigger(long t, long windowStartTime, float avgBw) {
        int size = computeCacheSize(t, windowStartTime, avgBw);
        if (cache.size() > size) {
            int evictionSize = cache.size() - size;
            long[] evictedEntries = new long[evictionSize];
            for (int i = 0; i < evictionSize; i++) {
                long key = cache.poll().key;
                evictedEntries[i] = key;
                keyHasArrived.remove(key);
            }
            return evictedEntries;
        }

        return new long[0];
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
        return CacheSizePolicies.computeEagerOptimalOnline(t, startTime, w, keyHasArrived, arrivalsHistories);
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
    public void nextWindow() throws Exception {
        nArrivalsPrevWindow = getnArrivals();
        nArrivals = 0;
        for (Key k : arrivalsHistories.values()) {
            // You can remove keys that has zero estimated arrival rates.
            k.nextWindow();
        }
        cacheKeys.clear();
        cache.clear();
        if (keyHasArrived.size() != 0)
            throw new Exception("Key checkings are inconsistent!");
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

package se.kth.edge;

import java.util.*;

/**
 * Created by Hooman on 2017-06-05.
 */
public class CacheManager {

    private final HashMap<Long, CacheEntry> cachedKeys = new HashMap<>();
    private final Set<Long> arrivedKeys = new HashSet<>();
    private final PriorityQueue<CacheEntry> cache;
    private final float laziness;
    private int w;
    private final SizePolicy sPolicy;
    public final static float DEFAULT_LAZINESS = 0.25f;
    private boolean specialPriority = false; // Non-coordinated-Keys first

    public boolean isSpecialPriority() {
        return specialPriority;
    }

    public void setSpecialPriority(boolean specialPriority) {
        this.specialPriority = specialPriority;
    }


    // candidate.
    public enum EvictionPolicy {
        LRU,
        LFU,
        /**
         * Least Expected Arrival Rate
         */
        LEAR,
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

        if (e == EvictionPolicy.LRU) {
            cache = new PriorityQueue<>(new RecentlyUsedComparator());
        } else if (e == EvictionPolicy.LFU) {
            cache = new PriorityQueue<>(new FrequentlyUsedComparator());
        } else {
            cache = new PriorityQueue<>(new ArrivalRateComparator());
        }
        this.sPolicy = s;
        this.w = window;
        this.laziness = laziness;
    }

    /**
     * @param kid
     * @param time
     */
    public void insert(long kid, long time, float estimatedRate) {
        insertKey(kid, time, false, estimatedRate);
    }

    /**
     * @param kid
     * @param time
     */
    public void insert(long kid, long time) {
        insertKey(kid, time, false, 0);
    }

    public void insertPriorityKey(long kid, long time, float estimatedRate) {
        insertKey(kid, time, true, estimatedRate);
    }

    public void insertPriorityKey(long kid, long time) {
        insertKey(kid, time, true, 0);
    }

    private void insertKey(long kid, long time, boolean specialPriority, float estimatedRate) {
        CacheEntry entry = null;
        if (cachedKeys.containsKey(kid)) {
            entry = cachedKeys.get(kid);
            cache.remove(entry);
            entry.numArrivals++;
        } else {
            entry = new CacheEntry(kid);
            entry.numArrivals = 1;
            cachedKeys.put(kid, entry);
        }
        entry.estimatedRate = estimatedRate;
        entry.specialPriority = specialPriority;
        entry.lastUpdateTime = time;
        cache.add(entry);
        arrivedKeys.add(kid);
    }

    /**
     * @param t
     * @param windowStartTime
     * @param avgBw
     * @return
     */
    public long[] trigger(long t, long windowStartTime, float avgBw, int currentArrivals, Map<Long, Key>
            arrivalsHistory) {
        int size = computeCacheSize(t, windowStartTime, avgBw, currentArrivals, arrivalsHistory);
        if (cache.size() > size) {
            int evictionSize = cache.size() - size;
            long[] evictedEntries = new long[evictionSize];
            for (int i = 0; i < evictionSize; i++) {
                CacheEntry c = cache.poll();
                long key = c.key;
                evictedEntries[i] = key;
                arrivedKeys.remove(key);
                cachedKeys.remove(key);
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
    public int computeCacheSize(long t, long startTime, float avgBw, int currentArrivals, Map<Long, Key>
            arrivalsHistory) {
        int cacheSize = 0;
        if (sPolicy == SizePolicy.HYBRID) {
            int eagerSize = this.computeEagerOptimal(t, startTime, arrivalsHistory);
            int lazySize = this.computeLazyOptimal(t, startTime, avgBw, currentArrivals);
            cacheSize = (int) (laziness * lazySize + (1 - laziness) * eagerSize);
        } else if (sPolicy == SizePolicy.EAGER) {
            cacheSize = this.computeEagerOptimal(t, startTime, arrivalsHistory);
        } else {
            cacheSize = this.computeLazyOptimal(t, startTime, avgBw, currentArrivals);
        }
        return cacheSize;
    }

    public int computeEagerOptimal(long t, long startTime, Map<Long, Key> arrivalsHistory) {
        return CacheSizePolicies.computeEagerOptimalOnline(t, startTime, w, arrivedKeys, arrivalsHistory);
    }

    public int computeLazyOptimal(long t, long startTime, float avgBw, int currentArrivals) {
        double avgArrivalRate = 0;
        if (t != startTime) {
            avgArrivalRate = (double) currentArrivals / (t - startTime);
        }
        return CacheSizePolicies.computeLazyOptimalOnline(t, startTime + w, avgArrivalRate, avgBw);
    }

    public int getCurrentCacheSize() {
        return cache.size();
    }

    public long[] endOfWindow() throws Exception {
        int size = cache.size();
        long[] remainingKeys = new long[size];
        for (int i = 0; i < size; i++) {
            long key = cache.poll().key;
            remainingKeys[i] = key;
            cachedKeys.remove(key);
            arrivedKeys.remove(key);
        }

        if (!arrivedKeys.isEmpty() || !cache.isEmpty() || !cachedKeys.isEmpty())
            throw new Exception("Key checkings are inconsistent!");

        return remainingKeys;
    }


    private class FrequentlyUsedComparator implements Comparator<CacheEntry> {

        public int compare(CacheEntry e1, CacheEntry e2) {
//            if (!isSpecialPriority() || (e1.specialPriority && e2.specialPriority)) {
            if (e1.numArrivals < e2.numArrivals) {
                return -1;
            }

            if (e1.numArrivals > e2.numArrivals) {
                return 1;
            }
//            } else if (e1.specialPriority) {
//                return 1;
//            } else if (e2.specialPriority) {
//                return -1;
//            }

            return 0;
        }
    }

    private class RecentlyUsedComparator implements Comparator<CacheEntry> {

        public int compare(CacheEntry e1, CacheEntry e2) {
//            if (!isSpecialPriority() || (e1.specialPriority && e2.specialPriority)) {
            if (e1.lastUpdateTime < e2.lastUpdateTime) {
                return -1;
            }

            if (e1.lastUpdateTime > e2.lastUpdateTime) {
                return 1;
            }
//            } else if (e1.specialPriority) {
//                return 1;
//            } else if (e2.specialPriority) {
//                return -1;
//            }

            return 0;
        }
    }

    private class ArrivalRateComparator implements Comparator<CacheEntry> {

        public int compare(CacheEntry e1, CacheEntry e2) {
//            if (!isSpecialPriority() || (e1.specialPriority && e2.specialPriority)) {
            if (e1.estimatedRate < e2.estimatedRate) {
                return -1;
            }

            if (e1.estimatedRate > e2.estimatedRate) {
                return 1;
            }
//            } else if (e1.specialPriority) {
//                return 1;
//            } else if (e2.specialPriority) {
//                return -1;
//            }

            return 0;
        }
    }
}

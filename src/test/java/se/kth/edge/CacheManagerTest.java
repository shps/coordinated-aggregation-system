package se.kth.edge;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Hooman on 2017-06-12.
 */
public class CacheManagerTest {

    @Test
    public void insert() throws Exception {
        CacheManager manager = new CacheManager(10, CacheManager.SizePolicy.EAGER, CacheManager.EvictionPolicy.LFU);
        manager.insert(0, 1);
        assert manager.getCurrentCacheSize() == 1;
        manager.insert(0, 2);
        assert manager.getCurrentCacheSize() == 1;
        manager.insert(1, 3);
        assert manager.getCurrentCacheSize() == 2;
    }

    @Test
    public void trigger() throws Exception {
        CacheManager manager = new CacheManager(10, CacheManager.SizePolicy.EAGER, CacheManager.EvictionPolicy.LFU);
        float[] weights = new float[]{1};
        Key k1 = new Key(0, weights);
        Key k2 = new Key(1, weights);
        int nArrivals = 5;
        manager.insert(0, 1);
        k1.increaseArrival();
        manager.insert(0, 2);
        k1.increaseArrival();
        manager.insert(0, 3);
        k1.increaseArrival();
        manager.insert(1, 3);
        k2.increaseArrival();
        manager.insert(1, 4);
        k2.increaseArrival();
        Map<Long, Key> arrivals = new HashMap<>();
        arrivals.put(k1.getId(), k1);
        arrivals.put(k2.getId(), k2);
        long[] entries = manager.trigger(4, 0, 0, nArrivals, arrivals);
        assert entries.length == 2;
        assert manager.getCurrentCacheSize() == 0;
        manager.endOfWindow();
        k1.nextWindow();
        k2.nextWindow();
        manager.insert(0, 11);
        k1.increaseArrival();
        manager.insert(0, 12);
        k1.increaseArrival();
        manager.insert(1, 12);
        k2.increaseArrival();
        nArrivals = 3;
        entries = manager.trigger(14, 10, 0, nArrivals, arrivals);
        assert entries.length == 1;
        assert entries[0] == 1; // Eviction policy LFU

        manager = new CacheManager(10, CacheManager.SizePolicy.EAGER, CacheManager.EvictionPolicy.LRU);
        k1.nextWindow();
        k2.nextWindow();
        manager.insert(0, 1);
        k1.increaseArrival();
        manager.insert(0, 2);
        k1.increaseArrival();
        manager.insert(0, 3);
        k1.increaseArrival();
        manager.insert(1, 3);
        k2.increaseArrival();
        manager.insert(1, 4);
        k2.increaseArrival();
        nArrivals = 5;
        entries = manager.trigger(4, 0, 0, nArrivals, arrivals);
        assert entries.length == 2;
        manager.endOfWindow();
        k1.nextWindow();
        k2.nextWindow();
        manager.insert(0, 11);
        k1.increaseArrival();
        manager.insert(0, 12);
        k1.increaseArrival();
        manager.insert(1, 13);
        k2.increaseArrival();
        nArrivals = 3;
        entries = manager.trigger(14, 10, 0, nArrivals, arrivals);
        assert entries.length == 1;
        assert entries[0] == 0; // Eviction policy LRU
        // Priority Test
//        manager.endOfWindow();
//        k1.nextWindow();
//        k2.nextWindow();
//        manager.insert(0, 1);
//        k1.increaseArrival();
//        manager.insert(0, 2);
//        k1.increaseArrival();
//        manager.insert(0, 3);
//        k1.increaseArrival();
//        manager.insert(1, 3);
//        k2.increaseArrival();
//        manager.insert(1, 4);
//        k2.increaseArrival();
//        nArrivals = 5;
//        entries = manager.trigger(4, 0, 0, nArrivals, arrivals);
//        assert entries.length == 2;
//        manager.endOfWindow();
//        manager.setSpecialPriority(true);
//        k1.nextWindow();
//        k2.nextWindow();
//        manager.insertPriorityKey(0, 11);
//        k1.increaseArrival();
//        manager.insertPriorityKey(0, 12);
//        k1.increaseArrival();
//        manager.insert(1, 13);
//        k2.increaseArrival();
//        nArrivals = 3;
//        entries = manager.trigger(14, 10, 0, nArrivals, arrivals);
//        assert entries.length == 1;
//        assert entries[0] == 1; // Eviction policy LRU
//
//        // Priority Disabled
//        manager.endOfWindow();
//        manager.setSpecialPriority(false);
//        k1.nextWindow();
//        k2.nextWindow();
//        manager.insert(0, 1);
//        k1.increaseArrival();
//        manager.insert(0, 2);
//        k1.increaseArrival();
//        manager.insert(0, 3);
//        k1.increaseArrival();
//        manager.insert(1, 3);
//        k2.increaseArrival();
//        manager.insert(1, 4);
//        k2.increaseArrival();
//        nArrivals = 5;
//        entries = manager.trigger(4, 0, 0, nArrivals, arrivals);
//        assert entries.length == 2;
//        manager.endOfWindow();
//        k1.nextWindow();
//        k2.nextWindow();
//        manager.insertPriorityKey(0, 11);
//        k1.increaseArrival();
//        manager.insertPriorityKey(0, 12);
//        k1.increaseArrival();
//        manager.insert(1, 13);
//        k2.increaseArrival();
//        nArrivals = 3;
//        entries = manager.trigger(14, 10, 0, nArrivals, arrivals);
//        assert entries.length == 1;
//        assert entries[0] == 0; // Eviction policy LRU
    }

    @Test
    public void endOfWindow() throws Exception {
        CacheManager manager = new CacheManager(10, CacheManager.SizePolicy.EAGER, CacheManager.EvictionPolicy.LFU);
        manager.insert(0, 1);
        manager.insert(1, 3);
        manager.endOfWindow();
        assert manager.getCurrentCacheSize() == 0;
//        assert manager.getnArrivals() == 0;
    }

}
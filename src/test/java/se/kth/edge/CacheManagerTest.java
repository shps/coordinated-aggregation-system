package se.kth.edge;

import org.junit.Test;

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
    public void addKeyArrival() throws Exception {
        CacheManager manager = new CacheManager(10, CacheManager.SizePolicy.EAGER, CacheManager.EvictionPolicy.LFU);
        manager.addKeyArrival(0, 1);
        assert manager.getnArrivals() == 1;
        manager.addKeyArrival(0, 2);
        assert manager.getnArrivals() == 2;
        manager.addKeyArrival(1, 3);
        assert manager.getnArrivals() == 3;
    }

    @Test
    public void trigger() throws Exception {
        CacheManager manager = new CacheManager(10, CacheManager.SizePolicy.EAGER, CacheManager.EvictionPolicy.LFU);
        manager.insert(0, 1);
        manager.insert(0, 2);
        manager.insert(0, 3);
        manager.insert(1, 3);
        manager.insert(1, 4);
        long[] entries = manager.trigger(4, 0, 0);
        assert entries.length == 2;
        assert manager.getCurrentCacheSize() == 0;
        manager.nextWindow();
        manager.insert(0, 11);
        manager.insert(0, 12);
        manager.insert(1, 12);
        entries = manager.trigger(14, 10, 0);
        assert entries.length == 1;
        assert entries[0] == 1; // Eviction policy LFU

        manager = new CacheManager(10, CacheManager.SizePolicy.EAGER, CacheManager.EvictionPolicy.LRU);
        manager.insert(0, 1);
        manager.insert(0, 2);
        manager.insert(0, 3);
        manager.insert(1, 3);
        manager.insert(1, 4);
        entries = manager.trigger(4, 0, 0);
        assert entries.length == 2;
        manager.nextWindow();
        manager.insert(0, 11);
        manager.insert(0, 12);
        manager.insert(1, 13);
        entries = manager.trigger(14, 10, 0);
        assert entries.length == 1;
        assert entries[0] == 0; // Eviction policy LRU
    }

    @Test
    public void nextWindow() throws Exception {
        CacheManager manager = new CacheManager(10, CacheManager.SizePolicy.EAGER, CacheManager.EvictionPolicy.LFU);
        manager.insert(0, 1);
        manager.insert(1, 3);
        manager.nextWindow();
        assert manager.getCurrentCacheSize() == 0;
        assert manager.getnArrivals() == 0;
    }

}
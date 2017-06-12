package se.kth.experiments;

import se.kth.stream.Tuple;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Hooman on 2017-06-12.
 */
public class LazyOptimalCacheStatistics {
    private final List<Long> triggerTimes = new LinkedList<>();
    private final List<List<Long>> triggeredKeys = new LinkedList<>();
    private final LinkedList<Integer> cacheSizes = new LinkedList();
    private final LinkedList<Integer> updateSizes = new LinkedList();

    public LazyOptimalCacheStatistics(LinkedList<Tuple> tuplesPerWindow) {
        computeOptimalCacheSizes(tuplesPerWindow);
    }

    public LazyOptimalCacheStatistics(LinkedList<Tuple> tuplesPerWindow, long time, int timestep, int window) {
        computeOptimalCacheSizes(tuplesPerWindow, time, timestep, window);
    }

    private void computeOptimalCacheSizes(LinkedList<Tuple> tuplesPerWindow, long time, int timestep, int window)
    {
        throw new UnsupportedOperationException();
    }

    private void computeOptimalCacheSizes(LinkedList<Tuple> tuplesPerWindow) {
        throw new UnsupportedOperationException();
    }
}

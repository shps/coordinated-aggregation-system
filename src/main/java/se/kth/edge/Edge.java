package se.kth.edge;

/**
 * Created by Hooman on 2017-06-24.
 */
public class Edge {

    private final int eId;
    private final WorkloadMonitor wMonitor;
    private final CacheManager cache;

    public Edge(int eId, CacheManager c, WorkloadMonitor m) {
        this.eId = eId;
        this.cache = c;
        this.wMonitor = m;
    }

    public void keyArrival(long kid, long time) {
        cache.insert(kid, time);
        wMonitor.addKeyArrival(kid, time);
    }

    public long[] trigger(long t, long windowStartTime, float avgBw) {
        long[] updates = cache.trigger(t, windowStartTime, avgBw, wMonitor.getnArrivals(), wMonitor
                .getArrivalsHistories());
        return updates;
    }

    public long[] endOfWindow() throws Exception {
        long[] remaingingKeys = cache.endOfWindow();
        wMonitor.endOfWindow();

        return remaingingKeys;
    }

    public CacheManager getCacheManager() {
        return cache;
    }
}

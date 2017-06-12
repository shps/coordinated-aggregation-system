package se.kth.edge;

/**
 * Created by Hooman on 2017-06-07.
 */
public class CacheEntry {

    public final long key;
    public int numArrivals;
    public long lastUpdateTime;

    public CacheEntry(long key)
    {
        this.key = key;
    }

}

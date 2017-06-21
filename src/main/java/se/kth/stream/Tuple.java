package se.kth.stream;

/**
 * Created by Hooman on 2017-06-07.
 */
public class Tuple implements Comparable<Tuple> {

    private final long key;
    private final long timestamp;

    /**
     * @param key
     * @param timestamp
     */
    public Tuple(long key, long timestamp) {
        this.key = key;
        this.timestamp = timestamp;
    }

    public long getKey() {
        return key;
    }

    public long getTimestamp() {
        return timestamp;
    }


    @Override
    public int compareTo(Tuple o) {
        if (this.getTimestamp() < o.getTimestamp()) {
            return -1;
        }

        if (this.getTimestamp() > o.getTimestamp()) {
            return 1;
        }

        return 0;
    }
}

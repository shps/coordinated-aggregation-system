package se.kth.stream;

import org.junit.Test;

import java.util.PriorityQueue;

import static org.junit.Assert.*;

/**
 * Created by Hooman on 2017-06-21.
 */
public class TupleTest {
    @Test
    public void compare() throws Exception {
        // Test if it's descending
        Tuple t1 = new Tuple(1, 3);
        Tuple t2 = new Tuple(2,2);
        Tuple t3 = new Tuple(3,1);
        PriorityQueue<Tuple> q = new PriorityQueue<>();
        q.add(t1);
        q.add(t2);
        q.add(t3);
        assert q.poll().getTimestamp() == t3.getTimestamp();
        assert q.poll().getTimestamp() == t2.getTimestamp();
        assert q.poll().getTimestamp() == t1.getTimestamp();
    }

}
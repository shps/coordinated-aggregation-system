package se.kth.edge;

import org.junit.Test;

/**
 * Created by Hooman on 2017-06-23.
 */
public class KeyTest {
    @Test
    public void getHistorySize() throws Exception {
        int size = 3;
        Key k = new Key(1, new float[size]);
        assert k.getHistorySize() == size;
    }

    @Test
    public void increaseArrival() throws Exception {
        int size = 1;
        float[] weights = new float[]{1};
        Key k = new Key(1, weights);
        k.increaseArrival();
        assert k.getCurrentArrival() == 1;
        k.increaseArrival();
        assert k.getCurrentArrival() == 2;
    }

    @Test
    public void nextWindow() throws Exception {
        int size = 2;
        float[] weights = new float[]{0.5f, 0.5f};
        Key k = new Key(1, weights);
        k.increaseArrival();
        k.increaseArrival();
        k.nextWindow();
        assert k.getCurrentArrival() == 0;
        assert k.getEstimatedArrivalRate() == 1;
        k.increaseArrival();
        k.increaseArrival();
        k.nextWindow();
        assert k.getEstimatedArrivalRate() == 2;
    }

}
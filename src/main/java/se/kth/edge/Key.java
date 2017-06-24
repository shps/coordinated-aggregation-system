package se.kth.edge;

import java.util.LinkedList;

/**
 * Created by Hooman on 2017-06-22.
 */
public class Key {

    private final long id;
    private final int[] arrivalHistory;
    private int pointer;
    private int currentArrival;
    private final float[] weights; // from the oldest to the latest
    private int estimatedArrivalRate = 0;

    /**
     * @param id
     * @param weights sorted from the oldest to the latest.
     */
    public Key(long id, float[] weights) {
        this.id = id;
        this.weights = weights;
        arrivalHistory = new int[weights.length];
        pointer = 0;
        currentArrival = 0;
    }


    public int getHistorySize() {
        return arrivalHistory.length;
    }

    public void increaseArrival() {
        currentArrival++;
    }

    public void nextWindow() {
        arrivalHistory[pointer] = currentArrival;
        pointer++;
        if (pointer % arrivalHistory.length == 0)
            pointer = 0;
        estimatedArrivalRate = computeEstimatedArrivalRate();
        currentArrival = 0;

    }


    private int computeEstimatedArrivalRate() {
        float arrival = 0;
        int index = pointer;
        for (int i = 0; i < arrivalHistory.length; i++) {
            arrival += weights[i] * arrivalHistory[index % arrivalHistory.length];
            index++;
        }

        return Math.round(arrival);
    }

    public int getEstimatedArrivalRate() {
        return estimatedArrivalRate;
    }

    public int getCurrentArrival() {
        return currentArrival;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Key key = (Key) o;

        return id == key.id;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    public long getId() {
        return id;
    }
}

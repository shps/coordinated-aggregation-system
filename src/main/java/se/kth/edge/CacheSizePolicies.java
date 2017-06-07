package se.kth.edge;

import java.util.Collection;
import java.util.List;

/**
 * Created by Hooman on 2017-06-05.
 */
public class CacheSizePolicies {

    public static int computeEagerOptimalOnline(long t, long T, int w, Collection<List<Long>> arrivals) {

        double t2 = (double) (t - T) / (double) w;
        double pSum = 0;

        for (List l : arrivals) {
            int w2 = w * l.size();
            pSum += 1 - Math.pow(t2, w2) - Math.pow(1 - t2, w2);
        }

        return (int) Math.ceil(pSum);
    }

    public static int computeLazyOptimalOnline(long t, long endTime, double avgArrivalRate, float avgBw) // pessimistic
    {
        int c = (int) ((avgBw * (endTime - t)) - computeRemainingCacheMisses(avgArrivalRate, t, endTime));

        return Math.max(c, 0);
    }

    // Compute integral, a definite integral for a constant value that is equal to calculate the area of a rectangle.
    private static double computeRemainingCacheMisses(double arrivalRate, long t, long endTime) {

        return (endTime - t) * arrivalRate;
    }

}

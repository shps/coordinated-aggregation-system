package se.kth.edge;

import java.util.Collection;
import java.util.List;

/**
 * Created by Hooman on 2017-06-05.
 */
public class CacheSizePolicies {

    public static double computeTBar(long t, long T, int w) {
        return (double) (t - T) / (double) w;
    }

    public static int computeEagerOptimalOnline(long t, long T, int w, Collection<List<Long>> arrivalsPerKey) {

        double tBar = computeTBar(t, T, w);
        double pSum = 0;

        for (List l : arrivalsPerKey) {

            pSum += arrivalProbability(tBar, l.size());
        }

        return (int) Math.round(pSum);
    }

    public static double arrivalProbability(double tBar, double lambda) {
//        double w2 = w * lambda;
        return 1 - Math.pow(tBar, lambda) - Math.pow(1 - tBar, lambda);
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

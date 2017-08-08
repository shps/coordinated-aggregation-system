package se.kth.edge;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by Hooman on 2017-06-05.
 */
public class CacheSizePolicies {

    public static double computeTBar(long t, long T, int w) {
        return (double) (t - T) / (double) w;
    }

    public static int computeEagerOptimalOnline(long t, long T, int w, Collection<Integer> arrivalsPerKey) {

        double tBar = computeTBar(t, T, w);
        double pSum = 0;

        for (int a : arrivalsPerKey) {

            pSum += arrivalProbability(tBar, a);
        }

        return (int) Math.round(pSum);
    }

    public static int computeEagerOptimalOnline(long t, long T, int w, Set<Long> arrivedKeys, Map<Long, Key>
            arrivalsPerKey) {

        double tBar = computeTBar(t, T, w);
        double pSum = 0;

        for (long k : arrivedKeys) {
            float arrivalRate = arrivalsPerKey.get(k).getEstimatedArrivalRate();
            if (arrivalRate > 0) {
                pSum += arrivalProbability(tBar, arrivalRate);
            }
        }

        return (int) Math.round(pSum);
    }

    public static double arrivalProbability(double tBar, float arrivalRate) {
        return 1 - Math.pow(tBar, arrivalRate) - Math.pow(1 - tBar, arrivalRate);
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

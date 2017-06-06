package se.kth.edge.updatemanagement;

import java.util.*;
import java.util.Map.Entry;

/**
 * Created by Hooman on 2017-06-05.
 */
public class KeyManager {

    private Map<Integer, List<Integer>> keyArrivals = new HashMap<>();
    private Map<Integer, List<Integer>> keyArrivalsPrevWindow = new HashMap<>();
    private int nArrivals = 0;
    private int nArrivalsPrevWindow = 0;
    private double laziness = 0.25;


    /**
     * @param kid
     * @param time arrival time
     */
    public void addKeyArrival(int kid, int time) {
        List<Integer> arrivalTimes;
        if (keyArrivals.containsKey(kid)) {
            arrivalTimes = keyArrivals.get(kid);
        } else {
            arrivalTimes = new LinkedList<Integer>();
            keyArrivals.put(kid, arrivalTimes);
        }

        arrivalTimes.add(time);
        nArrivals++;
    }

    // TODO: 2017-06-06  compute avgBw
    public int computeCacheSize(int t, int startTime, int w, float avgBw) {
        int eagerSize = CacheSizePolicies.computeEagerOptimalOnline(t, startTime, w, keyArrivalsPrevWindow.values());
        double avgArrivalRate = 0;
        if (t != startTime) {
            avgArrivalRate = (double) nArrivals / (t - startTime);
        }
        int lazySize = CacheSizePolicies.computeLazyOptimalOnline(t, startTime + w, avgArrivalRate, avgBw);
        int cacheSize = (int) (laziness * lazySize + (1 - laziness) * eagerSize);
        return cacheSize;
    }

    /**
     * Call this method upon the start of each window.
     */
    public void nextWindow() {
        keyArrivalsPrevWindow = keyArrivals;
        keyArrivals = new HashMap<>();
        nArrivalsPrevWindow = nArrivals;
        nArrivals = 0;
    }

}

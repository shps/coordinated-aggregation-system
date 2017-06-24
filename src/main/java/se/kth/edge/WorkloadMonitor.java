package se.kth.edge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Hooman on 2017-06-24.
 */
public class WorkloadMonitor {

    public final static int DEFAULT_HISTORY_SIZE = 1;
    public final static int DEFAULT_REGISTER_THRESHOLD = 5; // the arrival rate threshold for becoming a coordinator
    private final int registerThreshold;
    private final float beta;
    public final static float DEFAULT_BETA = 0.5f;
    private final Set<Long> registeredKeys = new HashSet<>();
    private final Set<Key> pendingForRegister = new HashSet<>();
    private final Set<Key> pendingUnRegister = new HashSet<>();
    private final Map<Long, Key> arrivalsHistories = new HashMap<>();
    private final float[] weights;
    private int nArrivals = 0;
    private int nArrivalsPrevWindow = 0;

    public WorkloadMonitor(int historySize, float beta) {
        this.beta = beta;
        weights = computeWeights(historySize, beta);
        this.registerThreshold = DEFAULT_REGISTER_THRESHOLD;
    }

    public float[] getWeights() {
        return weights;
    }

    public Set<Key> getPendingForRegister() {
        return pendingForRegister;
    }

    public Set<Key> getPendingUnRegister() {
        return pendingUnRegister;
    }


    private float[] computeWeights(int historySize, float beta) {
        float[] weights = new float[historySize];
        int size = weights.length;
        float[] wPrime = new float[size];
        float sum = 0;
        for (int i = 0; i < size; i++) {
            wPrime[i] = (float) Math.pow(beta, size - i + 1);
            sum += wPrime[i];
        }

        for (int i = 0; i < size; i++) {
            weights[i] = wPrime[i] / sum;
        }

        return weights;
    }

    /**
     * @param kid
     * @param time arrival time
     */
    public void addKeyArrival(long kid, long time) {
        Key k;
        if (getArrivalsHistories().containsKey(kid)) {
            k = getArrivalsHistories().get(kid);
        } else {
            k = new Key(kid, getWeights());
            getArrivalsHistories().put(kid, k);
        }
        k.increaseArrival();
        nArrivals = getnArrivals() + 1;
    }

    public int getnArrivals() {
        return nArrivals;
    }

    public void endOfWindow() {
        nArrivalsPrevWindow = getnArrivals();
        nArrivals = 0;
        getPendingUnRegister().clear();
        getPendingUnRegister().clear();
        for (Key k : getArrivalsHistories().values()) {
            // You can remove keys that has zero estimated arrival rates.
            k.nextWindow();
            if (k.getEstimatedArrivalRate() >= registerThreshold && !registeredKeys.contains(k.getId())) {
                getPendingForRegister().add(k);
                registeredKeys.add(k.getId());
            } else if (k.getEstimatedArrivalRate() < registerThreshold && registeredKeys.contains(k.getId())) { //
                // TODO put a percentage for threshold to avoid constant switching between register and unregister
                getPendingUnRegister().add(k);
                registeredKeys.remove(k.getId());
            }
        }
    }

    public Map<Long, Key> getArrivalsHistories() {
        return arrivalsHistories;
    }
}

package se.kth.center;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by Hooman on 2017-06-13.
 */
public class KeyManager {

    private final HashSet<Long>[] edgeKeys;
    private final int numEdges;
    private final HashMap<Long, Integer> keyEdgeCount = new HashMap<>();
    private final int[] keySimilarities;
    private int oCost;
    private int coCost;
    private int e2eUpdates;
    private int coCenterUpdates;
    private int interPrice = 2;
    private int intraPrice = 1;
    private int obCenterUpdates;

    public KeyManager(int numEdges) {
        this.numEdges = numEdges;
        keySimilarities = new int[numEdges];
        edgeKeys = new HashSet[numEdges];
        for (int i = 0; i < numEdges; i++) {
            edgeKeys[i] = new HashSet<>();
        }
    }

    public void update(int eId, long[] keys) {
        for (long k : keys) {
            if (!edgeKeys[eId].contains(k)) {
                edgeKeys[eId].add(k);
                int occurrences = 0;
                if (keyEdgeCount.containsKey(k)) {
                    occurrences = keyEdgeCount.get(k);
                }
                occurrences++;
                keyEdgeCount.put(k, occurrences);
            }
        }
    }

    public void onWindowEnd() {
        Collection<Integer> occurrences = keyEdgeCount.values();
        for (int o : occurrences) {
            getKeySimilarities()[o - 1]++;
//            oCost += o * interPrice;
//            coCost += interPrice + ((o - 1) * intraPrice);
            obCenterUpdates = getObCenterUpdates() + o;
            coCenterUpdates = getCoCenterUpdates() + 1;
            e2eUpdates = getE2eUpdates() + o - 1;
        }
        oCost = getObCenterUpdates() * interPrice;
        coCost = getCoCenterUpdates() * interPrice + getE2eUpdates() * intraPrice;
    }

    public void reset() {
        oCost = 0;
        coCost = 0;
        e2eUpdates = 0;
        coCenterUpdates = 0;
        obCenterUpdates = 0;
        keyEdgeCount.clear();
        for (int i = 0; i < getNumEdges(); i++) {
            edgeKeys[i].clear();
            getKeySimilarities()[i] = 0;
        }
    }

    public int getNumEdges() {
        return numEdges;
    }

    public int[] getKeySimilarities() {
        return keySimilarities;
    }

    public int getoCost() {
        return oCost;
    }

    public int getCoCost() {
        return coCost;
    }


    public int getInterPrice() {
        return interPrice;
    }

    public void setInterPrice(int interPrice) {
        this.interPrice = interPrice;
    }

    public int getIntraPrice() {
        return intraPrice;
    }

    public void setIntraPrice(int intraPrice) {
        this.intraPrice = intraPrice;
    }

    public int getCostDifference() {
        return oCost - coCost;
    }

    public int getE2eUpdates() {
        return e2eUpdates;
    }

    public int getCoCenterUpdates() {
        return coCenterUpdates;
    }

    public int getObCenterUpdates() {
        return obCenterUpdates;
    }
}

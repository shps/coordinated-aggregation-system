package se.kth.stream;

import java.util.*;

/**
 * Created by Hooman on 2017-06-14.
 */
public class SyntheticDataBuilder {

    private final int numEdgeDataCenters;
    private final int numKeys;
    private float[] kDistributions;
    private List<Set<Integer>>[] powerset;
    private final int window;
    private final float DEFAULT_MAX_RATE = 0.1f; // The rate compared to the window size
    private int minArrivalTime;

    public SyntheticDataBuilder(int numEdgeDataCenters, int numKeys, int window) {
        this.window = window;
        this.numEdgeDataCenters = numEdgeDataCenters;
        this.numKeys = numKeys;
        kDistributions = new float[numEdgeDataCenters];
        setDefaultKDistribution(getkDistributions(), numEdgeDataCenters); // TODO support distribution functions
        powerset = new List[numEdgeDataCenters + 1];
        for (int i = 0; i < powerset.length; i++) {
            powerset[i] = new LinkedList<>();
        }

        Set<Integer> edgeDataCenters = new HashSet<>();
        for (int i = 0; i < numEdgeDataCenters; i++) {
            edgeDataCenters.add(i);
        }

        createPowerSet(edgeDataCenters);
        minArrivalTime = (int) (window * DEFAULT_MAX_RATE);
    }

    private void setDefaultKDistribution(float[] kDistributions, int numEdgeDataCenters) {
        float p = (float) 1 / (float) numEdgeDataCenters;
        for (int i = 0; i < kDistributions.length; i++) {
            kDistributions[i] = p;
        }
    }

    public void setKDistribution(float[] kDistributions) throws Exception {
        if (kDistributions.length != this.getkDistributions().length)
            throw new Exception("The KDistributions array size does not match!");
        this.kDistributions = kDistributions;
    }

    public Set<KeyEntry>[] buildKeys() {
        Set<KeyEntry>[] keysPerEdge = new HashSet[numEdgeDataCenters];
        for (int i = 0; i < numEdgeDataCenters; i++) {
            keysPerEdge[i] = new HashSet<>();
        }
        int kid = 0;
        int maxTime = window - minArrivalTime;
        Random uniformRandom = new Random();
        for (int i = 0; i < getkDistributions().length; i++) {
            float p = getkDistributions()[i];
            int size = (int) Math.round(p * numKeys);
            // create all the subsets with size i+1.
            List<Set<Integer>> subsets = getSubsetsWithSize(i + 1);
            int numSubsets = subsets.size();
            for (int j = 0; j < size; j++) {
                int index = uniformRandom.nextInt(numSubsets);
                Set<Integer> edges = subsets.get(index);
                int id = kid + j;
                for (int e : edges) { // assign keys to the selected edges
                    // TODO add noise and make it independent from the window size.
                    int arrivalTime = minArrivalTime + uniformRandom.nextInt(maxTime);
                    KeyEntry k = new KeyEntry();
                    k.id = id;
                    k.arrivalTime = arrivalTime;
                    keysPerEdge[e].add(k);
                }
            }
            kid = kid + size;
        }

        return keysPerEdge;
    }

    public List<Set<Integer>> getSubsetsWithSize(int i) {
        return powerset[i];
    }

    private Set<Set<Integer>> createPowerSet(Set<Integer> set) {
        Set<Set<Integer>> sets = new HashSet<Set<Integer>>();
        if (set.isEmpty()) {
            Set<Integer> emptySet = new HashSet<>();
            sets.add(emptySet);
            powerset[0].add(emptySet);
            return sets;
        }


        List<Integer> elements = new ArrayList<>(set);
        int first = elements.get(0);
        set.remove(first);
        for (Set subsets : createPowerSet(set)) {
            Set<Integer> s = new HashSet<>();
            s.add(first);
            s.addAll(subsets);
            sets.add(s);
            sets.add(subsets);
            powerset[s.size()].add(s);
        }

        return sets;
    }

    public float[] getkDistributions() {
        return kDistributions;
    }
}

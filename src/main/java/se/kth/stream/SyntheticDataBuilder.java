package se.kth.stream;

import java.util.*;

/**
 * Created by Hooman on 2017-06-14.
 */
public class SyntheticDataBuilder {

    private final int numEdgeDataCenters;
    private final int numKeys;
    private float[] kDistributions; // TODO Support distributions
    private List<Set<Integer>>[] powerset;
    private final int maxArrivalRate = 100;
    private final KDistribution dist;

    public enum KDistribution {
        UNIFORM, ASCENDING_EXP, DESENDING_EXP, ALL_SIMILAR, NO_SIMILAR
    }

    public SyntheticDataBuilder(int numEdgeDataCenters, int numKeys, KDistribution dist) {
        this.dist = dist;
        this.numEdgeDataCenters = numEdgeDataCenters;
        this.numKeys = numKeys;
        kDistributions = new float[numEdgeDataCenters];
        setKDistribution(getkDistributions(), numEdgeDataCenters, this.dist);
        powerset = new List[numEdgeDataCenters + 1];
        for (int i = 0; i < powerset.length; i++) {
            powerset[i] = new LinkedList<>();
        }

        Set<Integer> edgeDataCenters = new HashSet<>();
        for (int i = 0; i < numEdgeDataCenters; i++) {
            edgeDataCenters.add(i);
        }

        createPowerSet(edgeDataCenters);
    }

    private void setKDistribution(float[] kDistributions, int numEdgeDataCenters, KDistribution dist) {
        if (dist == KDistribution.UNIFORM) {
            float p = (float) 1 / (float) numEdgeDataCenters;
            for (int i = 0; i < kDistributions.length; i++) {
                kDistributions[i] = p;
            }
        } else if (dist == KDistribution.ASCENDING_EXP) {
            float max = numEdgeDataCenters * numEdgeDataCenters;
            float prev = 0;
            for (int i = 1; i <= numEdgeDataCenters; i++) {
                float curr = i * i;
                kDistributions[i - 1] = (curr - prev) / max;
                prev = curr;
            }
        } else if (dist == KDistribution.ALL_SIMILAR) {
            for (int i = 0; i < kDistributions.length - 1; i++) {
                kDistributions[i] = 0;
            }
            kDistributions[kDistributions.length - 1] = 1;
        } else if (dist == KDistribution.NO_SIMILAR) {
            for (int i = 1; i < kDistributions.length; i++) {
                kDistributions[i] = 0;
            }
            kDistributions[0] = 1;
        }
    }

//    public void setKDistribution(float[] kDistributions) throws Exception {
//        if (kDistributions.length != this.getkDistributions().length)
//            throw new Exception("The KDistributions array size does not match!");
//        this.kDistributions = kDistributions;
//    }

    public Set<KeyEntry>[] buildKeys() {
        Set<KeyEntry>[] keysPerEdge = new HashSet[numEdgeDataCenters];
        for (int i = 0; i < numEdgeDataCenters; i++) {
            keysPerEdge[i] = new HashSet<>();
        }
        int kid = 0;
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
                    int arrivalRate = 1 + uniformRandom.nextInt(maxArrivalRate);
                    KeyEntry k = new KeyEntry();
                    k.id = id;
                    k.arrivalRate = arrivalRate;
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
        Set<Set<Integer>> sets = new HashSet();
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

package se.kth.stream;

import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Hooman on 2017-06-16.
 */
public class SyntheticDataBuilderTest {
    @Test
    public void setKDistribution() throws Exception {
    }

    @Test
    public void buildKeys() throws Exception {
        int numEdges = 3;
        int numKeys = 5;
        int window = 300;
        SyntheticDataBuilder builder = new SyntheticDataBuilder(numEdges, numKeys, window);
        Set<KeyEntry>[] entries = builder.buildKeys();
        int sizePerGroup = Math.round((float) numKeys / (float) numEdges);
        int numEntries = 0;
        for (int i = 1; i <= numEdges; i++) {
            numEntries += i * sizePerGroup;
        }

        int actualSize = 0;
        for (Set<KeyEntry> e : entries) {
            actualSize += e.size();
        }

        assert actualSize == numEntries;

        numKeys = 12;
        builder = new SyntheticDataBuilder(numEdges, numKeys, window);
        entries = builder.buildKeys();
        sizePerGroup = Math.round((float) numKeys / (float) numEdges);
        numEntries = 0;
        for (int i = 1; i <= numEdges; i++) {
            numEntries += i * sizePerGroup;
        }

        actualSize = 0;
        for (Set<KeyEntry> e : entries) {
            actualSize += e.size();
        }

        assert actualSize == numEntries;
    }

    @Test
    public void getSubsetsWithSize() throws Exception {
        int numEdges = 3;
        int numKeys = 10;
        int window = 300;
        SyntheticDataBuilder builder = new SyntheticDataBuilder(numEdges, numKeys, window);
        int count = 0;
        for (int i = 0; i <= numEdges; i++) {
            List<Set<Integer>> sets = builder.getSubsetsWithSize(i);
            count += sets.size();
        }

        assert count == Math.pow(2, numEdges);

        float p = (float) 1 / (float) numEdges;
        float[] kDistributions = builder.getkDistributions();
        assert kDistributions.length == numEdges;
        for (float d : kDistributions) { // Default distribution per key similarity group.
            assert d == p;
        }
    }

}
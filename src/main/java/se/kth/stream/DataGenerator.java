package se.kth.stream;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by Hooman on 2017-06-11.
 */
public class DataGenerator {

    public static void main(String[] args) throws IOException {
        int numEdges = 4;
        int numKeys = 1000;
        int window = 3600;
        int numWindow = 20;
        String outputFile = "/Users/ganymedian/Desktop/aggregation/";
        SyntheticDataBuilder builder = new SyntheticDataBuilder(numEdges, numKeys, SyntheticDataBuilder.KDistribution
                .ASCENDING_EXP);
        Set<KeyEntry>[] keys = builder.buildKeys();

        for (int i = 0; i < numEdges; i++) {
            generateData(keys[i], numWindow, window, String.format("%s%d", outputFile, i));
        }
    }

    /**
     * @param entries
     * @param numWindows
     * @param window
     * @param outputFile
     * @throws IOException
     */
    public static void generateData(Set<KeyEntry> entries, int numWindows, int window, String outputFile) throws
            IOException {
        FileMaker.writeKeyArrivals(String.format("%s-keys.txt", outputFile), entries);
        LinkedList<Tuple> tuples = SyntheticDataGenerator.generateDataWithPoissonDistribution(numWindows, window,
                entries);
        FileMaker.writeToFile(String.format("%s-stream.txt", outputFile), tuples);
    }
}
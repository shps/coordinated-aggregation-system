package se.kth.stream;

import se.kth.stream.uservisits.UserVisitsFileReader;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by Hooman on 2017-06-11.
 */
public class DataGenerator {

    public static void main(String[] args) throws IOException {
        int numEdges = 6;
        int numKeys = 1000;
        int window = 7200;
        int numWindow = 20;
        String outputFile = "/Users/ganymedian/Desktop/aggregation/";
        boolean userVisit = false;
        boolean fluctuateRates = true;
        if (!userVisit) {
            SyntheticDataBuilder builder = new SyntheticDataBuilder(numEdges, numKeys, SyntheticDataBuilder
                    .KDistribution

                    .ASCENDING_EXP);
            Set<KeyEntry>[] keys = builder.buildKeys();

            for (int i = 0; i < numEdges; i++) {
                generateData(keys[i], numWindow, window, String.format("%s%d", outputFile, i), fluctuateRates);
            }
        } else {
            for (int i = 0; i < numEdges; i++) {
                generateUserVisitPerEdge(i, String.format("%s%d", outputFile, i));
            }
        }
    }

    /**
     * @param entries
     * @param numWindows
     * @param window
     * @param outputFile
     * @throws IOException
     */
    public static void generateData(Set<KeyEntry> entries, int numWindows, int window, String outputFile, boolean
            fluctuateRates) throws
            IOException {
        FileMaker.writeKeyArrivals(String.format("%s-keys.csv", outputFile), entries);
//        LinkedList<Tuple> tuples = SyntheticDataGenerator.generateDataWithPoissonDistribution(numWindows, window,
//                entries);
        LinkedList<Tuple> tuples = SyntheticDataGenerator.generateDataWithFixedEventTime(numWindows * window, window,
                entries, fluctuateRates);
        FileMaker.writeToFile(String.format("%s-stream.csv", outputFile), tuples);
    }

    public static void generateUserVisitPerEdge(int eId, String outputFile) throws IOException {
        String file = "/Users/Ganymedian/Desktop/uservisits/part-0000";
        LinkedList<Tuple> visits = new LinkedList<>();
        for (int i = eId; i < eId + 1; i++)
            visits.addAll(UserVisitsFileReader.readTuple(String.format("%s%d", file, i)));

        FileMaker.writeToFile(String.format("%s-stream.csv", outputFile), visits);
    }
}

package se.kth.stream;

import se.kth.stream.KeyEntry;
import se.kth.stream.StreamFileMaker;
import se.kth.stream.SyntheticDataGenerator;
import se.kth.stream.Tuple;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by Hooman on 2017-06-11.
 */
public class DataGenerator {

    public static void main(String[] args) throws IOException {
        int duration = 10 * 3600; // 1 hour
        int[] arrivalTimes = new int[]{60, 10, 300, 600, 30, 150, 20, 80, 120, 350, 200, 70, 35, 25, 40, 120, 140, 160,
                180, 200, 220};
        String outputFile = "/Users/ganymedian/Desktop/aggregation/synthdataset.txt";

        generateData(arrivalTimes, duration, outputFile);
    }

    public static void generateData(int[] arrivalTimes, int duration, String outputFile) throws IOException {
        LinkedList<Tuple> tuples = SyntheticDataGenerator.generateDataWithPoissonDistribution(duration, arrivalTimes);
        StreamFileMaker.writeToFile(outputFile, tuples);
    }

    public static void generateData(Set<KeyEntry> entries, int duration, String outputFile) throws IOException {
        LinkedList<Tuple> tuples = SyntheticDataGenerator.generateDataWithPoissonDistribution(duration, entries);
        StreamFileMaker.writeToFile(outputFile, tuples);
    }
}

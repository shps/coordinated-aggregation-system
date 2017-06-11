package se.kth.experiments;

import se.kth.stream.StreamFileMaker;
import se.kth.stream.SyntheticDataGenerator;
import se.kth.stream.Tuple;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by Hooman on 2017-06-11.
 */
public class GenerateData {

    public static void main(String[] args) throws IOException {
        int duration = 10 * 3600; // 1 hour
        double[] lambdas = new double[]{60, 10, 300, 600, 30, 150, 20, 80, 120, 350, 200, 70, 35, 25, 40, 120, 140, 160, 180, 200, 220};
        String outputFile = "/Users/ganymedian/Desktop/aggregation/synthdataset.txt";

        LinkedList<Tuple> tuples = SyntheticDataGenerator.generateDataWithPoissonDistribution(duration, lambdas);
        StreamFileMaker.writeToFile(outputFile, tuples);
    }
}

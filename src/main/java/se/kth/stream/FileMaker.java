package se.kth.stream;

import java.io.*;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by Hooman on 2017-06-11.
 */
public class FileMaker {

    private static final String OUTPUT_DELIMITER = "\t";

    /**
     * @param outputFile
     * @param tuples
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void writeToFile(String outputFile, LinkedList<Tuple> tuples) throws FileNotFoundException,
            IOException {

        try (PrintWriter writer = new PrintWriter(new FileOutputStream(new File(outputFile)))) {
            for (Tuple t : tuples) {
                StringBuilder sb = new StringBuilder();
                sb.append(t.getTimestamp()).append(OUTPUT_DELIMITER).append(t.getKey());
                writer.println(sb.toString());
            }
            writer.flush();
        }
    }

    public static void writeKeyArrivals(String outputFile, Set<KeyEntry> keys) throws FileNotFoundException {
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(new File(outputFile)))) {
            for (KeyEntry k : keys) {
                StringBuilder sb = new StringBuilder();
                sb.append(k.id).append(OUTPUT_DELIMITER).append(k.arrivalRate);
                writer.println(sb.toString());
            }
            writer.flush();
        }
    }

}

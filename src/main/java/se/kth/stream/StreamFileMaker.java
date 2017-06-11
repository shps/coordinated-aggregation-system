package se.kth.stream;

import java.io.*;
import java.util.LinkedList;

/**
 * Created by Hooman on 2017-06-11.
 */
public class StreamFileMaker {

    private static final String OUTPUT_DELIMITER = "\t";

    /**
     *
     * @param outputFile
     * @param tuples
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void writeToFile(String outputFile, LinkedList<Tuple> tuples) throws FileNotFoundException, IOException {

        try (PrintWriter writer = new PrintWriter(new FileOutputStream(new File(outputFile)))) {
            for (Tuple t : tuples) {
                StringBuilder sb = new StringBuilder();
                sb.append(t.getTimestamp()).append(OUTPUT_DELIMITER).append(t.getKey());
                writer.println(sb.toString());
            }
            writer.flush();
        }
    }

}

package se.kth.stream;

import java.io.*;
import java.util.LinkedList;

/**
 * Created by Hooman on 2017-06-11.
 */
public class StreamFileReader {

    private final static String DEFAULT_DELIMITER = ",";

    /**
     *
     * @param file
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static LinkedList<Tuple> read(String file) throws FileNotFoundException, IOException {
        LinkedList<Tuple> tuples = new LinkedList<>();
        FileInputStream fis = new FileInputStream(new File(file));
        InputStreamReader isr = new InputStreamReader(fis);
        try (BufferedReader in = new BufferedReader(isr)) {
            String line;
            while ((line = in.readLine()) != null) {
                String values[] = line.split(DEFAULT_DELIMITER);
                int timestamp = Integer.parseInt(values[0]);
                int key = Integer.parseInt(values[1]);
                Tuple t = new Tuple(key, timestamp);
                tuples.add(t);
            }
        }

        return tuples;
    }
}

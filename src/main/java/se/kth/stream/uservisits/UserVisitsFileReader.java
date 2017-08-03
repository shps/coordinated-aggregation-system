package se.kth.stream.uservisits;

import se.kth.stream.Tuple;

import java.io.*;
import java.util.LinkedList;

/**
 * Created by Hooman on 2017-08-03.
 */
public class UserVisitsFileReader {

    private final static String DEFAULT_DELIMITER = ",";

    /**
     * @param file
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static LinkedList<UserVisit> readUserVisit(String file) throws FileNotFoundException, IOException {
        LinkedList<UserVisit> tuples = new LinkedList<>();
        FileInputStream fis = new FileInputStream(new File(file));
        InputStreamReader isr = new InputStreamReader(fis);
        int counter = 0;
        try (BufferedReader in = new BufferedReader(isr)) {
            String line;
            while ((line = in.readLine()) != null) {
                counter++;
                String values[] = line.split(DEFAULT_DELIMITER);
                UserVisit v = new UserVisit();
                v.sourceIP = values[0];
                v.destURL = values[1];
                v.visitDate = values[2];
                v.adRevenue = Float.valueOf(values[3]);
                v.userAgent = values[4];
                v.countryCode = values[5];
                v.languageCode = values[6];
                v.searchWord = values[7];
                v.duration = Integer.valueOf(values[8]);
                v.timestamp = counter;
                tuples.add(v);
            }

        }

        return tuples;
    }


    public static LinkedList<Tuple> readTuple(String file) throws FileNotFoundException, IOException {
        LinkedList<Tuple> tuples = new LinkedList<>();
        FileInputStream fis = new FileInputStream(new File(file));
        InputStreamReader isr = new InputStreamReader(fis);
        int counter = 0;
        try (BufferedReader in = new BufferedReader(isr)) {
            String line;
            while ((line = in.readLine()) != null) {
                counter++;
                String values[] = line.split(DEFAULT_DELIMITER);
                Tuple t = new Tuple(values[7].hashCode(), counter);
                tuples.add(t);
            }

        }

        return tuples;
    }
}

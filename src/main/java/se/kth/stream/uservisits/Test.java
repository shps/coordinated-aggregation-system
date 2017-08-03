package se.kth.stream.uservisits;

import se.kth.stream.Tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by Hooman on 2017-08-03.
 */
public class Test {

    public static void main(String[] args) throws IOException {
        String file = "/Users/Ganymedian/Desktop/uservisits/part-0000";
        LinkedList<Tuple> visits = new LinkedList<>();
        for (int i = 0; i < 1; i++)
            visits.addAll(UserVisitsFileReader.readTuple(String.format("%s%d", file, i)));
//        HashMap<String, Integer> map = new HashMap();
//        int n= 0;
//        for (UserVisit u : visits) {
//            n++;
//            String k = u.searchWord;
////            String k = u.visitDate;
//            int counter = 0;
//            if (map.containsKey(k)) {
//                counter = map.get(k);
//            }
//
//            counter++;
//            map.put(k, counter);
//            if (n == 7200)
//                System.out.println(map.size());
//        }

    }
}

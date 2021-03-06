package se.kth.stream;

import java.util.*;

/**
 * Created by Hooman on 2017-06-11.
 */
public class SyntheticDataGenerator {


    public static LinkedList<Tuple> generateDataWithPoissonDistribution(int numWindows, int window, Set<KeyEntry>
            entries) {
//        PriorityQueue<Tuple>[] events = new PriorityQueue[entries.size()];
        SortedMap<Integer, LinkedList<Tuple>> allTuples = new TreeMap<>();
        Random r = new Random();
//        int i = 0;
        for (KeyEntry entry : entries) {
//            events[i] = new PriorityQueue<>();
            int t = 0;
            for (int w = 0; w < numWindows; w++) {
                int nextArrivalRate = Poisson.getPoisson(entry.arrivalRate);
                for (int j = 0; j < nextArrivalRate; j++) {
                    int arrivalTime = t + r.nextInt(window); // Uniform random generator
                    Tuple nextTuple = new Tuple(entry.id, arrivalTime);
//                    events[i].add(nextTuple);
                    LinkedList<Tuple> tuples;
                    if (!allTuples.containsKey(arrivalTime)) {
                        tuples = new LinkedList<>();
                        allTuples.put(arrivalTime, tuples);
                    } else {
                        tuples = allTuples.get(arrivalTime);
                    }
                    tuples.add(nextTuple);
                }
                t += window;
            }
//            i++;
        }

        return sortTuples(allTuples);
    }

    private static LinkedList<Tuple> sortTuples(SortedMap<Integer, LinkedList<Tuple>> allTuples) {
        LinkedList<Tuple> sortedTuples = new LinkedList<>();
        for (LinkedList<Tuple> tuples : allTuples.values()) {
            sortedTuples.addAll(tuples);
        }
        return sortedTuples;
    }

    private static LinkedList<Tuple> sortTuples(PriorityQueue<Tuple>[] events) {
        LinkedList<Tuple> tuples = new LinkedList<>();
        while (true) {
            long minTimestamp = Long.MAX_VALUE;
            int minIndex = -1;
            for (int i = 0; i < events.length; i++) {
                if (!events[i].isEmpty()) {
                    if (events[i].peek().getTimestamp() <= minTimestamp) {
                        minTimestamp = events[i].peek().getTimestamp();
                        minIndex = i;
                    }
                }
            }
            if (minIndex != -1) {
                tuples.add(events[minIndex].poll());
            } else {
                break;
            }
        }

        return tuples;
    }

    public static LinkedList<Tuple> generateDataWithFixedEventTime(int time, int window, Set<KeyEntry>
            entries, boolean fluctuatingRate) {
        SortedMap<Integer, LinkedList<Tuple>> allTuples = new TreeMap<>();
        Random r = new Random();
        for (KeyEntry entry : entries) {
            int t = 0;
            double rate = (double) entry.arrivalRate;
            double arrivalRate = rate / (double) window;
            int arrivalChange = window / 8;
            while (t < time) {
                double nextEventTime = -Math.log(1.0 - r.nextDouble()) / arrivalRate;
                t = (int) (t + Math.round(nextEventTime));
//                System.out.println(String.format("NExt Event Time: %d, Time: %d", entry.arrivalRate, t));
                Tuple nextTuple = new Tuple(entry.id, t);
                LinkedList<Tuple> tuples;
                if (!allTuples.containsKey(t)) {
                    tuples = new LinkedList<>();
                    allTuples.put(t, tuples);
                } else {
                    tuples = allTuples.get(t);
                }
                tuples.add(nextTuple);
                if (fluctuatingRate)
                    if (t / arrivalChange >= 1) {
                        arrivalChange += arrivalChange;
                        rate = ((rate + 40) % 200) + 1;
                        arrivalRate = rate / (double) window;
                    }
            }
        }

        return sortTuples(allTuples);
    }


}


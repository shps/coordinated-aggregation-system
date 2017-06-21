package se.kth.stream;

import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

/**
 * Created by Hooman on 2017-06-11.
 */
public class SyntheticDataGenerator {


    public static LinkedList<Tuple> generateDataWithPoissonDistribution(int numWindows, int window, Set<KeyEntry>
            entries) {
        PriorityQueue<Tuple>[] events = new PriorityQueue[entries.size()];
        Random r = new Random();
        int i = 0;
        for (KeyEntry entry : entries) {
            events[i] = new PriorityQueue<>();
            int t = 0;
            for (int w = 0; w < numWindows; w++) {
                int nextArrivalRate = Poisson.getPoisson(entry.arrivalRate);
                for (int j = 0; j < nextArrivalRate; j++) {
                    int arrivalTime = t + r.nextInt(window); // Uniform random generator
                    Tuple nextTuple = new Tuple(entry.id, arrivalTime);
                    events[i].add(nextTuple);
                }
                t += window;
            }
            i++;
        }

        return sortTuples(events);
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

}


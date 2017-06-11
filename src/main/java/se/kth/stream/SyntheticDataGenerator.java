package se.kth.stream;

import java.util.LinkedList;

/**
 * Created by Hooman on 2017-06-11.
 */
public class SyntheticDataGenerator {

    public static LinkedList<Tuple> generateDataWithPoissonDistribution(int duration, double[] lambdas) {
        LinkedList<Tuple>[] events = new LinkedList[lambdas.length];
        for (int i = 0; i < lambdas.length; i++) {
            int t = 0;
            events[i] = new LinkedList<>();
            while ((t += Poisson.getPoisson(lambdas[i])) <= duration) {
                Tuple nextTuple = new Tuple(i, t);
                events[i].add(nextTuple);
            }
        }

        LinkedList<Tuple> tuples = new LinkedList<>();
        while (true) {
            long minTimestamp = Long.MAX_VALUE;
            int minIndex = -1;
            for (int i = 0; i < events.length; i++) {
                if (!events[i].isEmpty()) {
                    if (events[i].peekFirst().getTimestamp() <= minTimestamp) {
                        minTimestamp = events[i].peekFirst().getTimestamp();
                        minIndex = i;
                    }
                }
            }
            if (minIndex != -1) {
                tuples.add(events[minIndex].pollFirst());
            } else {
                break;
            }
        }

        return tuples;
    }

}


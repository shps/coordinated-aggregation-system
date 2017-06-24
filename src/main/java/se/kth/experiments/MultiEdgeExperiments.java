package se.kth.experiments;

import se.kth.center.KeyManager;
import se.kth.edge.CacheManager;
import se.kth.edge.Edge;
import se.kth.edge.WorkloadMonitor;
import se.kth.stream.StreamFileReader;
import se.kth.stream.Tuple;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Hooman on 2017-06-12.
 */
public class MultiEdgeExperiments {

    static final Edge[] edges;
    static int numEdges = 4;
    static int timestep = 200;
    static int window = 3600;
    static int windowCounter;
    private final static float alpha = 0.25f;
    private final static float avgBw = 1;
    private final static LinkedList<Tuple>[] tuplesPerWindow;
    private final static LinkedList<Long> triggerTimes;
    private static final HashSet<Long>[] keysPerWindow;
    private final static LinkedList<Integer>[] eCacheSizes;
    private final static LinkedList<Integer>[] eUpdateSize;
    private final static int[] eUpdatesPerWindow;
    private final static String inputFile = "/Users/ganymedian/Desktop/aggregation/";
    private static int totalKeys;
    private static int totalUpdates;
    private static int totalArrivals;
    private static final KeyManager center;
    private static final int DEFAULT_INTER_PRICE = 3;
    private static final int DEFAULT_INTRA_PRICE = 1;

    static {
        center = new KeyManager(numEdges);
        center.setInterPrice(DEFAULT_INTER_PRICE);
        center.setIntraPrice(DEFAULT_INTRA_PRICE);
        tuplesPerWindow = new LinkedList[numEdges];
        triggerTimes = new LinkedList<>();
        keysPerWindow = new HashSet[numEdges];
        eCacheSizes = new LinkedList[numEdges];
        eUpdateSize = new LinkedList[numEdges];
        eUpdatesPerWindow = new int[numEdges];
        edges = new Edge[numEdges];

        for (int i = 0; i < numEdges; i++) {
            tuplesPerWindow[i] = new LinkedList<>();
            keysPerWindow[i] = new HashSet<>();
            eCacheSizes[i] = new LinkedList<>();
            eUpdateSize[i] = new LinkedList<>();
            CacheManager cache = new CacheManager(window, CacheManager.SizePolicy.EAGER, CacheManager.EvictionPolicy
                    .LFU);
            WorkloadMonitor monitor = new WorkloadMonitor();
            edges[i] = new Edge(i, cache, monitor);
        }
    }

    public static void main(String[] args) throws Exception {
        LinkedList<Tuple>[] streams = new LinkedList[numEdges];
        // load the streams from files
        for (int i = 0; i < numEdges; i++) {
            streams[i] = StreamFileReader.read(String.format("%s%d-stream.txt", inputFile, i));
            System.out.println(String.format("Number of tuples: %d", streams[i].size()));
        }

        timestepExecution(streams, timestep);
    }

    private static void timestepExecution(LinkedList<Tuple>[] streams, int timestep) throws Exception {

        windowCounter = 0;

        long time = 0;
        boolean windowStarts = true;

        while (true) {
            if (windowStarts) {
                resetOnWindowStart();
                windowStarts = false;
            }

            triggerTimes.add(time);
            for (int i = 0; i < numEdges; i++) {
                processNextTimeStep(i, time);
            }

            if (time == ((windowCounter + 1) * window)) // end of the window
            {
                windowStarts = true;
                boolean emptyStream = false;
                for (int i = 0; i < numEdges; i++) {
                    endOfWindow(i, time);
                    if (streams[i].isEmpty())
                        emptyStream = true;
                }
                printMultiEdgeStatistics(windowCounter);
                center.onWindowEnd();
                printCenterStatistics(center, windowCounter);
                windowCounter++;
                if (emptyStream) {
                    break;
                }
            } else {

                time += timestep;
                boolean endOfStreams = false;

                for (int i = 0; i < numEdges; i++) {
                    streamNextTimeStep(i, time, streams);
                    if (streams[i].isEmpty()) {
                        endOfStreams = true;
                    }
                }

                if (endOfStreams) {
                    break;
                }
            }
        }

    }

    private static void printCenterStatistics(KeyManager center, int windowCounter) {
        System.out.println(String.format("****** W%d ******", windowCounter));
        System.out.println(String.format("Key Similarities 1:%d: %s", numEdges, Arrays.toString(center
                .getKeySimilarities())));
        System.out.println(String.format("Oblivious Cost: %d, Coordinated Cost: %d, Cost Difference: %d, Saving: %f",
                center
                        .getoCost(), center.getCoCost(), center.getCostDifference(), 1.0 - ((float) center.getCoCost() /
                        (float) center
                                .getoCost())));
    }

    private static void streamNextTimeStep(int eId, long time, LinkedList<Tuple>[] streams) {
        while (streams[eId].peek() != null && (streams[eId].peek().getTimestamp() < time)) {
            Tuple t = streams[eId].poll();
            keysPerWindow[eId].add(t.getKey());
            tuplesPerWindow[eId].add(t);
            edges[eId].keyArrival(t.getKey(), t.getTimestamp());
        }
    }

    private static void endOfWindow(int eId, long time) throws Exception {
        int uKeys = keysPerWindow[eId].size();
        totalKeys += uKeys;
        eUpdatesPerWindow[eId] += edges[eId].endOfWindow().length;
        totalUpdates += eUpdatesPerWindow[eId];
        int nArrivals = tuplesPerWindow[eId].size();
        totalArrivals += nArrivals;
        // Last tuple in the current window

        // Jump to the end of the window
        // Set for a new window.
        EagerOptimalCacheStatistics statistics = new EagerOptimalCacheStatistics(tuplesPerWindow[eId], time - window,
                timestep, window);
        System.out.println(String.format("******* E%d Summary *******", eId));
        System.out.println(String.format("End of w%d", windowCounter));
        System.out.println(String.format("BW Cost of Batching: %d", uKeys));
        System.out.println(String.format("BW Cost of Streaming: %d", nArrivals));
        System.out.println(String.format("Eager: BW Cost = %d, Estimated Cache Sizes: %s", eUpdatesPerWindow[eId],
                Arrays.toString(eCacheSizes[eId].toArray())));
        System.out.println(String.format("Optimal: BW Cost = %d, Cache Sizes: %s", statistics.getTotalUpdates(),
                Arrays.toString(statistics.getCacheSizes().toArray())));
        System.out.println("**************");
//        writeToFile(triggerTimes, windowCounter + 1, eCacheSizes[eId], eUpdateSize[eId], eWriter);
//        writeToFile(triggerTimes, windowCounter + 1, statistics.getCacheSizes(), statistics.getUpdateSizes(),
// oWriter);
    }

    private static void printMultiEdgeStatistics(int windowCounter) {
        System.out.println(String.format("***** Summary of W%d *****", windowCounter));
        System.out.println(String.format("Total Keys: %d", totalKeys));
        System.out.println(String.format("Total Arrivals: %d", totalArrivals));
        System.out.println(String.format("Total Updates: %d", totalUpdates));
    }

    private static void processNextTimeStep(int eId, long time) {
        long[] eUpdates = edges[eId].trigger(time, windowCounter * window, avgBw);
        center.update(eId, eUpdates);
        final int eSize = edges[eId].getCacheManager().getCurrentCacheSize();
        eCacheSizes[eId].add(eSize);
        eUpdateSize[eId].add(eUpdates.length);
        eUpdatesPerWindow[eId] += eUpdates.length;
    }

    private static void writeToFile(List<Long> triggerTimes, int window, List<Integer> cSizes, List<Integer> uSize,
                                    PrintWriter writer) {
        for (int i = 0; i < triggerTimes.size(); i++) {
            writer.append(String.format("%d,%d,%d,%d", triggerTimes.get(i), window, cSizes.get(i), uSize.get(i)));
            writer.append("\n");
        }
    }

    private static void resetOnWindowStart() throws Exception {
        center.reset();
        totalArrivals = 0;
        totalKeys = 0;
        totalUpdates = 0;
        triggerTimes.clear();
        for (int i = 0; i < numEdges; i++) {
            tuplesPerWindow[i].clear();
            eCacheSizes[i].clear();
            eUpdateSize[i].clear();
            eUpdatesPerWindow[i] = 0;
            keysPerWindow[i].clear();
        }
    }

}

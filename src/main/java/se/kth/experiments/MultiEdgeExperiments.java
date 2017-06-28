package se.kth.experiments;

import se.kth.center.KeyManager;
import se.kth.edge.*;
import se.kth.stream.StreamFileReader;
import se.kth.stream.Tuple;

import java.io.PrintWriter;
import java.util.*;

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
    private final static Map<Integer, Map<Integer, List<Long>>> edgeToEdgeUpdates;
    private final static int[] eUpdatesPerWindow;
    private final static int[] cUpdatesPerWindow;
    private final static String inputFile = "/Users/ganymedian/Desktop/aggregation/";
    private static int totalKeys;
    private static int totalUpdates;
    private static int totalCenterUpdates;
    private static int totalEdgeUpdates;
    private static int totalArrivals;
    private static final KeyManager center;
    private static final int DEFAULT_INTER_PRICE = 3;
    private static final int DEFAULT_INTRA_PRICE = 1;
    private final static Coordinator coordinator;
    private static int sanityCounter;
    private static boolean sendFinalStepToEdge = false;

    static {
        center = new KeyManager(numEdges);
        center.setInterPrice(DEFAULT_INTER_PRICE);
        center.setIntraPrice(DEFAULT_INTRA_PRICE);
        tuplesPerWindow = new LinkedList[numEdges];
        triggerTimes = new LinkedList<>();
        keysPerWindow = new HashSet[numEdges];
        eCacheSizes = new LinkedList[numEdges];
//        eUpdateSize = new LinkedList[numEdges];
//        eeUpdateSize = new LinkedList[numEdges];
//        cUpdateSize = new LinkedList[numEdges];
        eUpdatesPerWindow = new int[numEdges];
        cUpdatesPerWindow = new int[numEdges];
        edges = new Edge[numEdges];
        coordinator = new Coordinator(numEdges);
        edgeToEdgeUpdates = new HashMap<>();

        for (int i = 0; i < numEdges; i++) {
            tuplesPerWindow[i] = new LinkedList<>();
            keysPerWindow[i] = new HashSet<>();
            eCacheSizes[i] = new LinkedList<>();
//            eUpdateSize[i] = new LinkedList<>();
            CacheManager cache = new CacheManager(window, CacheManager.SizePolicy.EAGER, CacheManager.EvictionPolicy
                    .LFU);
            WorkloadMonitor monitor = new WorkloadMonitor();
            edges[i] = new Edge(i, cache, monitor);
            edgeToEdgeUpdates.put(i, new HashMap<>());
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
                edgeToEdgeUpdates.put(i, processNextTimeStep(i, time));
            }

            if (time == ((windowCounter + 1) * window)) // end of the window
            {
                windowStarts = true;
                boolean emptyStream = false;
                for (int i = 0; i < numEdges; i++) {
                    streamEdgeToEdgeTimeStep(i, time - 1, edgeToEdgeUpdates.get(i));
                    endOfWindow(i, time);
                    if (streams[i].isEmpty())
                        emptyStream = true;
                }
                printMultiEdgeStatistics(windowCounter);
                center.onWindowEnd();
                printCenterStatistics(center, windowCounter);
                Map<Long, Integer> newCoordinators = coordinator.computeNewCoordinators();
                // Report updated coordinators for keys to all edges (This can be asynchronous)
                for (Edge e : edges) {
                    e.updateCoordinators(newCoordinators);
                }
                windowCounter++;
                if (emptyStream) {
                    break;
                }
            } else {
                time += timestep;
                boolean endOfStreams = false;

                for (int i = 0; i < numEdges; i++) {
                    streamNextTimeStep(i, time, streams);
                    streamEdgeToEdgeTimeStep(i, time - 1, edgeToEdgeUpdates.get(i));
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
                center.getoCost(), center.getCoCost(), center.getCostDifference(), 1.0 - ((float) center.getCoCost() /
                        (float) center.getoCost())));
    }

    private static void streamNextTimeStep(int srcEdge, long time, LinkedList<Tuple>[] streams) {
        while (streams[srcEdge].peek() != null && (streams[srcEdge].peek().getTimestamp() < time)) {
            Tuple t = streams[srcEdge].poll();
            deliverTupleTo(srcEdge, srcEdge, t);
        }
//        System.out.println(String.format("Edge %d sends %d updates.", srcEdge, e2eUpdates));
    }

    private static void streamEdgeToEdgeTimeStep(int srcEdge, long time, Map<Integer, List<Long>>
            edgeUpdates) {
        // delivering the updates for other edges from the previous time step in the end of the current time step
        int e2eUpdates = 0;
        for (int dstEdge : edgeUpdates.keySet()) {
            long[] keys = convertToLongArray(edgeUpdates.get(dstEdge));
            for (long key : keys) {
                Tuple t = new Tuple(key, time);
                deliverTupleTo(srcEdge, dstEdge, t);
                e2eUpdates++;
            }
        }
        eUpdatesPerWindow[srcEdge] += e2eUpdates;
    }

    private static void deliverTupleTo(final int srcEdge, final int dstEdge, final Tuple t) {
        keysPerWindow[dstEdge].add(t.getKey());
        tuplesPerWindow[dstEdge].add(t);
        edges[dstEdge].keyArrival(t.getKey(), t.getTimestamp(), srcEdge);
    }

    private static Map<Integer, List<Long>> processNextTimeStep(int eId, long time) {
        long[] allUpdates = edges[eId].trigger(time, windowCounter * window, avgBw);

        sanityCounter += allUpdates.length;
        long[] updatesToCenter;
        Map<Integer, List<Long>> updatesPerEdge;
        if (time % window != 0 || sendFinalStepToEdge) {
            //Separate the updates that are toward the center from the updates toward the other edges.
            // The updates for the edges will be processed in the next time step.
            updatesPerEdge = edges[eId].getKeyCoordinator(allUpdates);
            if (updatesPerEdge.containsKey(eId)) {
                updatesToCenter = convertToLongArray(updatesPerEdge.get(eId)); // the overhead for list to primitve
                updatesPerEdge.remove(eId);
            } else {
                updatesToCenter = new long[0];
            }
        } else {
            updatesPerEdge = new HashMap<>();
            updatesToCenter = allUpdates;
        }

        // array conversion
        // will be fixed later. I'm not changing the types because of the later network-based implementation.
        center.update(eId, updatesToCenter);
        final int eSize = edges[eId].getCacheManager().getCurrentCacheSize();
        eCacheSizes[eId].add(eSize);
//        eUpdateSize[eId].add(allUpdates.length);
        cUpdatesPerWindow[eId] += updatesToCenter.length;
//        cUpdateSize[eId].add(updatesToCenter.length);
        //TODO add statistics for edge to edge communication

        return updatesPerEdge;
    }

    private static void endOfWindow(int eId, long time) throws Exception {
        // TODO for now we ignore edge to edge communication of the last time step.
        int uKeys = keysPerWindow[eId].size();
        totalKeys += uKeys;
        long[] finalUpdates = edges[eId].endOfWindow();
        sanityCounter += finalUpdates.length;
        cUpdatesPerWindow[eId] += finalUpdates.length;
        totalUpdates += cUpdatesPerWindow[eId] + eUpdatesPerWindow[eId];
        totalCenterUpdates += cUpdatesPerWindow[eId];
        totalEdgeUpdates += eUpdatesPerWindow[eId];
        int nArrivals = tuplesPerWindow[eId].size();
        totalArrivals += nArrivals;

        //communications with the coordinator
//        for (Edge e : edges) {
        Set<Key> pendingKeys = edges[eId].getWorkloadManager().getPendingForRegister();
        long[] keys = new long[pendingKeys.size()];
        int[] arrivals = new int[pendingKeys.size()];
        int i = 0;
        for (Key k : pendingKeys) {
            keys[i] = k.getId();
            arrivals[i] = k.getEstimatedArrivalRate();
            i++;
        }
        Set<Long> unregister = edges[eId].getWorkloadManager().getPendingUnRegister();
        long[] keysToUnregister = new long[unregister.size()];
        i = 0;
        for (long k : unregister) {
            keysToUnregister[i] = k;
            i++;
        }
        coordinator.registerKeys(edges[eId].getId(), keys, arrivals);
        coordinator.unregisterKeys(edges[eId].getId(), keysToUnregister);
//        }

        // After all the edges report (Not necessary to wait for all the edges)

        EagerOptimalCacheStatistics statistics = new EagerOptimalCacheStatistics(tuplesPerWindow[eId], time - window,
                timestep, window);
        System.out.println(String.format("******* E%d Summary *******", eId));
        System.out.println(String.format("End of w%d", windowCounter));
        System.out.println(String.format("BW Cost of Batching: %d", uKeys));
        System.out.println(String.format("BW Cost of Streaming: %d", nArrivals));
        System.out.println(String.format("Eager: BW Cost: E2E: %d, Center: %d, Estimated Cache Sizes: %s",
                eUpdatesPerWindow[eId], cUpdatesPerWindow[eId], Arrays.toString(eCacheSizes[eId].toArray())));
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
        System.out.println(String.format("Total Updates (e2e=%d + center=%d): %d", totalEdgeUpdates,
                totalCenterUpdates, totalUpdates));
        System.out.println(String.format("Total Updates Sanity Check: %d", sanityCounter));
        int e2eCost = DEFAULT_INTRA_PRICE * totalEdgeUpdates;
        int centerCost = DEFAULT_INTER_PRICE * totalCenterUpdates;
        System.out.println(String.format("Total Cost= %d, E2E(%d) + Center(%d)", e2eCost + centerCost, e2eCost,
                centerCost));
    }

    private static long[] convertToLongArray(List<Long> list) {
        long[] array = new long[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }

        return array;
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
        totalCenterUpdates = 0;
        totalEdgeUpdates = 0;
        triggerTimes.clear();
        for (int i = 0; i < numEdges; i++) {
            tuplesPerWindow[i].clear();
            eCacheSizes[i].clear();
//            eUpdateSize[i].clear();
            eUpdatesPerWindow[i] = 0;
            cUpdatesPerWindow[i] = 0;
            keysPerWindow[i].clear();
        }
        edgeToEdgeUpdates.clear();
        sanityCounter = 0;
    }

}

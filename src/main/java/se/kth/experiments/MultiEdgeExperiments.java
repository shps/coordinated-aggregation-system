package se.kth.experiments;

import se.kth.center.KeyManager;
import se.kth.edge.*;
import se.kth.stream.StreamFileReader;
import se.kth.stream.Tuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by Hooman on 2017-06-12.
 */
public class MultiEdgeExperiments {

    static final Edge[] edges;
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
    private final static Coordinator coordinator;
    private static int sanityCounter;
    private static PrintWriter summaryPrinter;
    private static PrintWriter arrivalPrinter;
    private static PrintWriter optimalPrinter;
    private static PrintWriter edgePrinter;
    private static PrintWriter logger;
    private static int[] e2eCounter;
    static int numEdges = 6;
    static int timestep = 25;
    static int window = 3600;
    static int windowCounter;
    private final static float laziness = 0.15f;
    private final static float avgBw = 15f;
    private static final int DEFAULT_INTER_PRICE = 3;
    private static final int DEFAULT_INTRA_PRICE = 1;
    private static final boolean sendFinalStepToEdge = false;
    private static final boolean enableEdgeToEdge = true;
    private static final boolean SINGLE_AGGREGATION_POINT = false;
    private static final boolean priorityKeys = false; // TODO the current strategy is not improving results.
    private static final CacheManager.SizePolicy DEFAULT_SIZE_POLICY = CacheManager.SizePolicy.HYBRID;
    private static final CacheManager.EvictionPolicy DEFAULT_EVICTION_POLICY = CacheManager.EvictionPolicy
            .LFU;
    private static final int DEFAULT_HISTORY_SIZE = 1;
    private static final float DEFAULT_BETA = 0.9f;
    private static final int DEFAULT_REGISTER_THRESHOLD = 5;
    private static final float DEFAULT_UNREGISTER_PERCENTAGE = 0.15f;
    private static final Coordinator.SelectionStrategy DEFAULT_COORDINATOR_SELECTION = Coordinator.SelectionStrategy
            .MAX_ARRIVAL;
    private static final WorkloadMonitor.WeightType DEFAULT_WEIGHT_TYPE = WorkloadMonitor.WeightType.FADING;
    private static final int DEFAULT_AGGREGATION_POINT = 0;

    static {
        StringBuilder sBuilder = new StringBuilder(String.format("%ssummary-w%d", inputFile, window));
        StringBuilder s2Builder = new StringBuilder(String.format("%soptimal-w%d", inputFile, window));
        StringBuilder s3Builder = new StringBuilder(String.format("%sedges-w%d", inputFile, window));
        StringBuilder s4Builder = new StringBuilder(String.format("%slog-w%d", inputFile, window));
        StringBuilder arrivalFile = new StringBuilder(String.format("%sarrival-w%d.csv", inputFile, window));
        if (enableEdgeToEdge) {
            sBuilder.append("-e2e");
            s2Builder.append("-e2e");
            s3Builder.append("-e2e");
            s3Builder.append("-e2e");
        }
        if (sendFinalStepToEdge) {
            sBuilder.append("-withstep2e");
            s2Builder.append("-withstep2e");
            s3Builder.append("-withstep2e");
        }
        if (priorityKeys) {
            sBuilder.append("-priority");
            s3Builder.append("-priority");
        }
        sBuilder.append(".csv");
        s2Builder.append(".csv");
        s3Builder.append(".csv");
        s4Builder.append(".txt");
        try {
            summaryPrinter = new PrintWriter(new FileOutputStream(new File(sBuilder.toString())));
            summaryPrinter.append("window-counter,w,edges,total-keys,total-arrivals,total-e-updates,total-c-updates," +
                    "total-updates,total-cost,key-registers,key-removals,co-long-messages,co-int-messages," +
                    "co-edge-updates,edge-long-messages,edge-int-messages").append("\n");
            arrivalPrinter = new PrintWriter(new FileOutputStream(new File(arrivalFile.toString())));
            arrivalPrinter.append("edge-id,window-counter,key,estimated-arrival,current-arrival").append("\n");
            edgePrinter = new PrintWriter(new FileOutputStream(new File(s3Builder.toString())));
            edgePrinter.append("window-counter,edge-id,key-coordination,e2e-updates").append("\n");
            optimalPrinter = new PrintWriter(new FileOutputStream(new File(s2Builder.toString())));
            optimalPrinter.append("window-counter,w,edges,ob-center-updates,e2e-center-updates,e2e-updates,oblivious," +
                    "coordinated,gain").append("\n");
            logger = new PrintWriter(new FileOutputStream(new File(s4Builder.toString())));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
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
        coordinator = new Coordinator(numEdges, DEFAULT_COORDINATOR_SELECTION);
        edgeToEdgeUpdates = new HashMap<>();

        for (int i = 0; i < numEdges; i++) {
            tuplesPerWindow[i] = new LinkedList<>();
            keysPerWindow[i] = new HashSet<>();
            eCacheSizes[i] = new LinkedList<>();
//            eUpdateSize[i] = new LinkedList<>();
            CacheManager cache = new CacheManager(window, DEFAULT_SIZE_POLICY, DEFAULT_EVICTION_POLICY, laziness);
            cache.setSpecialPriority(priorityKeys);
            WorkloadMonitor monitor = new WorkloadMonitor(DEFAULT_HISTORY_SIZE, DEFAULT_BETA,
                    DEFAULT_REGISTER_THRESHOLD, DEFAULT_UNREGISTER_PERCENTAGE, enableEdgeToEdge, DEFAULT_WEIGHT_TYPE);
            edges[i] = new Edge(i, cache, monitor);
            edgeToEdgeUpdates.put(i, new HashMap<>());
            e2eCounter = new int[numEdges];
        }
    }

    public static void main(String[] args) throws Exception {


        printConfigurations();
        LinkedList<Tuple>[] streams = new LinkedList[numEdges];
        // load the streams from files
        for (int i = 0; i < numEdges; i++) {
            streams[i] = StreamFileReader.read(String.format("%s%d-stream.csv", inputFile, i));
            String s = String.format("Number of tuples: %d", streams[i].size());
            System.out.println(s);
            logger.println(s);
        }

        timestepExecution(streams, timestep);

        summaryPrinter.flush();
        summaryPrinter.close();
        optimalPrinter.flush();
        optimalPrinter.close();
        edgePrinter.flush();
        edgePrinter.close();
        logger.flush();
        logger.close();
        arrivalPrinter.flush();
        arrivalPrinter.close();
    }

    private static void printConfigurations() {

        String s = String.format("numEdges:%d\ntimestep:%d\nwindow:%d\nlaziness:%f\navgBw:%f\nInterPrice:%d" +
                        "\nIntraPrice:%d" +
                        "\nsendFinalStepToEdge:%s\nenableEdgeToEdge:%s\npriorityKeys:%s\nDEFAULT_SIZE_POLICY:%s" +
                        "\nDEFAULT_EVICTION_POLICY:%s\nDEFAULT_HISTORY_SIZE:%d\nDEFAULT_BETA:%f" +
                        "\nDEFAULT_REGISTER_THRESHOLD" +
                        ":%d\nDEFAULT_UNREGISTER_PERCENTAGE:%f\nDEFAULT_COORDINATOR_SELECTION:%s" +
                        "\nSINGLE_AGGREGATION_POINT:%s", numEdges, timestep,
                window, laziness, avgBw, DEFAULT_INTER_PRICE, DEFAULT_INTRA_PRICE, String.valueOf
                        (sendFinalStepToEdge), String.valueOf(enableEdgeToEdge), String.valueOf(priorityKeys),
                DEFAULT_SIZE_POLICY.toString(), DEFAULT_EVICTION_POLICY.toString(), DEFAULT_HISTORY_SIZE,
                DEFAULT_BETA, DEFAULT_REGISTER_THRESHOLD, DEFAULT_UNREGISTER_PERCENTAGE,
                DEFAULT_COORDINATOR_SELECTION.toString(), String.valueOf(SINGLE_AGGREGATION_POINT));
        System.out.println(s);
        logger.println(s);
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
                Map<Long, Integer> newCoordinators = coordinator.computeNewCoordinators();
                // Report updated coordinators for keys to all edges (This can be asynchronous)
                for (Edge e : edges) {
                    e.updateCoordinators(newCoordinators);
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
        String s1 = String.format("****** W%d ******", windowCounter);
        String s2 = String.format("Key Similarities 1:%d: %s", numEdges, Arrays.toString(center
                .getKeySimilarities()));
        String s3 = String.format("Oblivious Updates: %d, E2E Center Updates: %d, E2E Updates: %d, Oblivious " +
                        "Cost: %d, Coordinated Cost: %d, Cost Difference: %d, Saving: %f",
                center.getObCenterUpdates(), center.getCoCenterUpdates(), center.getE2eUpdates(), center.getoCost(),
                center.getCoCost(), center.getCostDifference(), 1.0 - ((float) center.getCoCost() / (float) center
                        .getoCost()));
        System.out.println(s1);
        System.out.println(s2);
        System.out.println(s3);
        logger.println(s1);
        logger.println(s2);
        logger.println(s3);
        optimalPrinter.append(String.format("%d,%d,%d,%d,%d,%d,%d,%d,%f", windowCounter, window, numEdges, center
                .getObCenterUpdates(), center.getCoCenterUpdates(), center.getE2eUpdates(), center.getoCost(), center
                .getCoCost(), 1.0 - ((float) center.getCoCost() / (float) center.getoCost()))).append("\n");
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
            e2eCounter[dstEdge] += keys.length;
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
            if (!SINGLE_AGGREGATION_POINT) {
                updatesPerEdge = edges[eId].getKeyCoordinator(allUpdates);
            } else {
                updatesPerEdge = new HashMap<>();
                List<Long> edgeKeys = new LinkedList();
                for (long k : allUpdates) {
                    edgeKeys.add(k);
                }
                updatesPerEdge.put(DEFAULT_AGGREGATION_POINT, edgeKeys);
            }

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
        if (keys.length > 0) {
            coordinator.registerKeys(edges[eId].getId(), keys, arrivals);
        }
        if (keysToUnregister.length > 0) {
            coordinator.unregisterKeys(edges[eId].getId(), keysToUnregister);
        }
//        }

        // After all the edges report (Not necessary to wait for all the edges)

        EagerOptimalCacheStatistics statistics = new EagerOptimalCacheStatistics(tuplesPerWindow[eId], time - window,
                timestep, window);
        String s1 = String.format("******* E%d Summary *******", eId);
        String s2 = String.format("End of w%d", windowCounter);
        String s3 = String.format("BW Cost of Batching: %d", uKeys);
        String s4 = String.format("BW Cost of Streaming: %d", nArrivals);
        String s5 = String.format("Eager: BW Cost: E2E: %d, Center: %d, Estimated Cache Sizes: %s",
                eUpdatesPerWindow[eId], cUpdatesPerWindow[eId], Arrays.toString(eCacheSizes[eId].toArray()));
        String s6 = String.format("Optimal: BW Cost = %d, Cache Sizes: %s", statistics.getTotalUpdates(),
                Arrays.toString(statistics.getCacheSizes().toArray()));
        String s7 = "**************";
        System.out.println(s1);
        System.out.println(s2);
        System.out.println(s3);
        System.out.println(s4);
        System.out.println(s5);
        System.out.println(s6);
        System.out.println(s7);
        logger.println(s1);
        logger.println(s2);
        logger.println(s3);
        logger.println(s4);
        logger.println(s5);
        logger.println(s6);
        logger.println(s7);
        for (int j = 0; j < edges.length; j++) {
            Map<Long, Key> arrivalHistories = edges[j].getWorkloadManager().getArrivalsHistories();
            for (Key k : arrivalHistories.values()) {
                String record = String.format("%d,%d,%d,%d,%d", j, windowCounter, k.getId(), k
                        .getEstimatedArrivalRate(), k
                        .getCurrentArrival());
                arrivalPrinter.append(record).append("\n");
            }
        }
        //        writeToFile(triggerTimes, windowCounter + 1, eCacheSizes[eId], eUpdateSize[eId], eWriter);
//        writeToFile(triggerTimes, windowCounter + 1, statistics.getCacheSizes(), statistics.getUpdateSizes(),
// oWriter);
    }

    private static void printMultiEdgeStatistics(int windowCounter) {
        int e2eCost = DEFAULT_INTRA_PRICE * totalEdgeUpdates;
        int centerCost = DEFAULT_INTER_PRICE * totalCenterUpdates;
        int totalCoUpdates = 0;
        int totalLongMessages = 0;
        int totalIntMessages = 0;
        for (Edge e : edges) {
            totalCoUpdates += e.getCoordinatorMessages();
            totalLongMessages += e.getLongMessages();
            totalIntMessages += e.getIntMessages();
        }
        summaryPrinter.append(String.format("%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d", windowCounter, window,
                numEdges, totalKeys,
                totalArrivals, totalEdgeUpdates, totalCenterUpdates, totalUpdates, e2eCost + centerCost,
                coordinator.getRegCounter(), coordinator.getRemCounter(), coordinator.getLongMessages(), coordinator
                        .getIntMessages(), totalCoUpdates, totalLongMessages, totalIntMessages)).append("\n");
        String s1 = String.format("***** Summary of W%d *****", windowCounter);
        String s2 = String.format("Total Keys: %d", totalKeys);
        String s3 = String.format("Total Arrivals: %d", totalArrivals);
        String s4 = String.format("Total Updates (e2e=%d + center=%d): %d", totalEdgeUpdates,
                totalCenterUpdates, totalUpdates);
        String s5 = String.format("Total Updates Sanity Check: %d", sanityCounter);
        String s6 = String.format("Total Cost= %d, E2E(%d) + Center(%d)", e2eCost + centerCost, e2eCost,
                centerCost);
        String s7 = String.format("Edge Coordination Load: %s", Arrays.toString(coordinator.getEdgeKeyCounters
                ()));
        String s8 = String.format("E2E Updates Load: %s", Arrays.toString(e2eCounter));
        System.out.println(s1);
        System.out.println(s2);
        System.out.println(s3);
        System.out.println(s4);
        System.out.println(s5);
        System.out.println(s6);
        System.out.println(s7);
        System.out.println(s8);
        logger.println(s1);
        logger.println(s2);
        logger.println(s3);
        logger.println(s4);
        logger.println(s5);
        logger.println(s6);
        logger.println(s7);
        logger.println(s8);
        int[] edgeCounters = coordinator.getEdgeKeyCounters();
        for (int i = 0; i < numEdges; i++) {
            edgePrinter.append(String.format("%d,%d,%d,%d\n", windowCounter, i, edgeCounters[i], e2eCounter[i]));
        }

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
        coordinator.resetStatistics();
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
            e2eCounter[i] = 0;
            keysPerWindow[i].clear();
            edges[i].resetStatistics();
        }
        edgeToEdgeUpdates.clear();
        sanityCounter = 0;
    }

}

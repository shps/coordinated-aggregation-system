package se.kth.experiments;

import se.kth.edge.CacheManager;
import se.kth.edge.Edge;
import se.kth.edge.WorkloadMonitor;
import se.kth.stream.StreamFileReader;
import se.kth.stream.Tuple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Hooman on 2017-06-11.
 */
public class SingleEdgeExperiments {

    private final static float alpha = 0.25f;
    private final static float avgBw = 1;
    private final static int window = 100;
    private final static int timestep = 10;
    private final static LinkedList<Tuple> tuplesPerWindow = new LinkedList<>();
    private final static LinkedList<Integer> eCacheSizes = new LinkedList<>();
    private final static LinkedList<Integer> lCacheSizes = new LinkedList<>();
    private final static LinkedList<Integer> hCacheSizes = new LinkedList<>();
    private final static LinkedList<Integer> eUpdateSize = new LinkedList<>();
    private final static LinkedList<Integer> lUpdateSize = new LinkedList<>();
    private final static LinkedList<Integer> hUpdateSize = new LinkedList<>();
    private final static LinkedList<Long> triggerTimes = new LinkedList<>();
    private final static CacheManager eCache = new CacheManager(window, CacheManager.SizePolicy.EAGER, CacheManager
            .EvictionPolicy.LFU, alpha);
    private final static CacheManager lCache = new CacheManager(window, CacheManager.SizePolicy.LAZY, CacheManager
            .EvictionPolicy.LFU, alpha);
    private final static CacheManager hCache = new CacheManager(window, CacheManager.SizePolicy.HYBRID,
            CacheManager.EvictionPolicy.LFU, alpha);
    private final static Edge eManager = new Edge(0, eCache, new WorkloadMonitor(WorkloadMonitor.DEFAULT_HISTORY_SIZE,
            WorkloadMonitor.DEFAULT_BETA));
    private final static Edge lManager = new Edge(0, lCache, new WorkloadMonitor(WorkloadMonitor.DEFAULT_HISTORY_SIZE,
            WorkloadMonitor.DEFAULT_BETA));
    private final static Edge hManager = new Edge(0, hCache, new WorkloadMonitor(WorkloadMonitor.DEFAULT_HISTORY_SIZE,
            WorkloadMonitor.DEFAULT_BETA));

    private final static String inputFile = "/Users/ganymedian/Desktop/aggregation/0-stream.txt";
    private final static String eOutputFile = "/Users/ganymedian/Desktop/aggregation/eoutput.txt";
    private final static String lOutputFile = "/Users/ganymedian/Desktop/aggregation/loutput.txt";
    private final static String hOutputFile = "/Users/ganymedian/Desktop/aggregation/houtput.txt";
    private final static String oOutputFile = "/Users/ganymedian/Desktop/aggregation/ooutput.txt";
    private static PrintWriter eWriter;
    private static PrintWriter lWriter;
    private static PrintWriter hWriter;
    private static PrintWriter oWriter;
    static int eUpdatesPerWindow;
    static int lUpdatesPerWindow;
    static int hUpdatesPerWindow;
    private static final HashSet<Long> keysPerWindow = new HashSet<>();

    public static void main(String[] args) throws Exception {

        eWriter = new PrintWriter(new FileOutputStream(new File(eOutputFile)));
        lWriter = new PrintWriter(new FileOutputStream(new File(lOutputFile)));
        hWriter = new PrintWriter(new FileOutputStream(new File(hOutputFile)));
        oWriter = new PrintWriter(new FileOutputStream(new File(oOutputFile)));

        String header = "timestamp,window,cachesize,updatesize\n";
        eWriter.write(header);
        lWriter.write(header);
        hWriter.write(header);
        oWriter.write(header);
        LinkedList<Tuple> tuples = StreamFileReader.read(inputFile);
        System.out.println(String.format("Number of tuples: %d", tuples.size()));

        timestepExecution(tuples, timestep);

        eWriter.flush();
        lWriter.flush();
        hWriter.flush();
        oWriter.flush();
        eWriter.close();
        lWriter.close();
        hWriter.close();
        oWriter.close();

    }

    private static void timestepExecution(LinkedList<Tuple> tuples, int timestep) throws Exception {

        int windowCounter = 0;

        long time = 0;
        boolean windowStarts = true;

        while (true) {
            if (windowStarts) {
                resetOnWindowStart();
                windowStarts = false;
            }

            //If timestep
            long[] eUpdates = eManager.trigger(time, windowCounter * window, avgBw);
            long[] lUpdates = lManager.trigger(time, windowCounter * window, avgBw);
            long[] hUpdates = hManager.trigger(time, windowCounter * window, avgBw);
            triggerTimes.add(time);

            final int eSize = eManager.getCacheManager().getCurrentCacheSize();
            eCacheSizes.add(eSize);
            final int lSize = lManager.getCacheManager().getCurrentCacheSize();
            lCacheSizes.add(lSize);
            final int hSize = hManager.getCacheManager().getCurrentCacheSize();
            hCacheSizes.add(hSize);

            eUpdateSize.add(eUpdates.length);
            lUpdateSize.add(lUpdates.length);
            hUpdateSize.add(hUpdates.length);
            eUpdatesPerWindow += eUpdates.length;
            lUpdatesPerWindow += lUpdates.length;
            hUpdatesPerWindow += hUpdates.length;

            if (time == ((windowCounter + 1) * window)) // end of the window
            {
                int uKeys = keysPerWindow.size();
                eUpdatesPerWindow += eSize;
                lUpdatesPerWindow += lSize;
                hUpdatesPerWindow += hSize;
                int nArrivals = tuplesPerWindow.size();
                // Last tuple in the current window

                // Jump to the end of the window
                // Set for a new window.
                windowStarts = true;
                EagerOptimalCacheStatistics statistics = new EagerOptimalCacheStatistics(tuplesPerWindow, time -
                        window, timestep, window);
                System.out.println(String.format("End of w%d", windowCounter));
                System.out.println(String.format("BW Cost of Batching: %d", uKeys));
                System.out.println(String.format("BW Cost of Streaming: %d", nArrivals));
                System.out.println(String.format("Eager: BW Cost = %d, Estimated Cache Sizes: %s", eUpdatesPerWindow,
                        Arrays.toString(eCacheSizes.toArray())));
                System.out.println(String.format("Lazy: BW Cost = %d, Estimated Cache Sizes: %s", lUpdatesPerWindow,
                        Arrays.toString(lCacheSizes.toArray())));
                System.out.println(String.format("Hybrid: BW Cost = %d, Estimated Cache Sizes: %s",
                        hUpdatesPerWindow, Arrays.toString(hCacheSizes.toArray())));
                System.out.println(String.format("Optimal: BW Cost = %d, Cache Sizes: %s", statistics.getTotalUpdates
                        (), Arrays.toString(statistics.getCacheSizes().toArray())));
                writeToFile(triggerTimes, windowCounter + 1, eCacheSizes, eUpdateSize, eWriter);
                writeToFile(triggerTimes, windowCounter + 1, lCacheSizes, lUpdateSize, lWriter);
                writeToFile(triggerTimes, windowCounter + 1, hCacheSizes, hUpdateSize, hWriter);
                writeToFile(triggerTimes, windowCounter + 1, statistics.getCacheSizes(), statistics.getUpdateSizes(),
                        oWriter);
                windowCounter++;
                if (tuples.isEmpty()) {
                    break;
                }
            } else {

                time += timestep;

                while (tuples.peek() != null && (tuples.peek().getTimestamp() < time)) {
                    Tuple t = tuples.poll();
                    keysPerWindow.add(t.getKey());
                    tuplesPerWindow.add(t);
                    eManager.keyArrival(t.getKey(), t.getTimestamp());
                    lManager.keyArrival(t.getKey(), t.getTimestamp());
                    hManager.keyArrival(t.getKey(), t.getTimestamp());
                }
            }
        }

    }

    private static void tupleStepExecution(LinkedList<Tuple> tuples) throws Exception {
        int windowCounter = 0;

        boolean windowStarts = true;

        long time = 0; //TODO create time step
        for (int i = 0; i < tuples.size() - 1; i++) {
            if (windowStarts) {
                resetOnWindowStart();
            }

            Tuple t1 = tuples.get(i); // current event
            Tuple t2 = tuples.get(i + 1); // next event
            tuplesPerWindow.add(t1);
            eManager.keyArrival(t1.getKey(), t1.getTimestamp());
            lManager.keyArrival(t1.getKey(), t1.getTimestamp());
            hManager.keyArrival(t1.getKey(), t1.getTimestamp());

            //If timestep
            long[] eUpdates = eManager.trigger(t1.getTimestamp(), windowCounter * window, avgBw);
            long[] lUpdates = lManager.trigger(t1.getTimestamp(), windowCounter * window, avgBw);
            long[] hUpdates = hManager.trigger(t1.getTimestamp(), windowCounter * window, avgBw);
            triggerTimes.add(t1.getTimestamp());

            int eSize = eManager.getCacheManager().getCurrentCacheSize();
            eCacheSizes.add(eSize);
            int lSize = lManager.getCacheManager().getCurrentCacheSize();
            lCacheSizes.add(lSize);
            int hSize = hManager.getCacheManager().getCurrentCacheSize();
            hCacheSizes.add(hSize);

            eUpdateSize.add(eUpdates.length);
            lUpdateSize.add(lUpdates.length);
            hUpdateSize.add(hUpdates.length);

            if (t1.getTimestamp() / window != t2.getTimestamp() / window) // t1 is the last arrival in the current
            // window.
            {
                // Last tuple in the current window

                // Jump to the end of the window
                // Set for a new window.

                windowStarts = true;
                EagerOptimalCacheStatistics statistics = new EagerOptimalCacheStatistics(tuplesPerWindow);
                System.out.println(String.format("End of w%d", windowCounter));
                System.out.println(String.format("Estimated Eager Cache Sizes: %s", Arrays.toString(eCacheSizes
                        .toArray())));
                System.out.println(String.format("Estimated Lazy Cache Sizes: %s", Arrays.toString(lCacheSizes
                        .toArray())));
                System.out.println(String.format("Estimated Hybrid Cache Sizes: %s", Arrays.toString(hCacheSizes
                        .toArray())));
                System.out.println(String.format("Optimal Cache Sizes: %s", Arrays.toString(statistics.getCacheSizes
                        ().toArray())));
                writeToFile(triggerTimes, windowCounter + 1, eCacheSizes, eUpdateSize, eWriter);
                writeToFile(triggerTimes, windowCounter + 1, lCacheSizes, lUpdateSize, lWriter);
                writeToFile(triggerTimes, windowCounter + 1, hCacheSizes, hUpdateSize, hWriter);
                writeToFile(triggerTimes, windowCounter + 1, statistics.getCacheSizes(), statistics.getUpdateSizes(),
                        oWriter);
                windowCounter++;
            } else {
                windowStarts = false;
            }
        }
    }


    private static void resetOnWindowStart() throws Exception {
        tuplesPerWindow.clear();
        eManager.endOfWindow();
        lManager.endOfWindow();
        hManager.endOfWindow();
        eCacheSizes.clear();
        lCacheSizes.clear();
        hCacheSizes.clear();
        triggerTimes.clear();
        eUpdateSize.clear();
        lUpdateSize.clear();
        hUpdateSize.clear();
        eUpdatesPerWindow = 0;
        lUpdatesPerWindow = 0;
        hUpdatesPerWindow = 0;
        keysPerWindow.clear();


    }

    private static void writeToFile(List<Long> triggerTimes, int window, List<Integer> cSizes, List<Integer> uSize,
                                    PrintWriter writer) {
        for (int i = 0; i < triggerTimes.size(); i++) {
            writer.append(String.format("%d,%d,%d,%d", triggerTimes.get(i), window, cSizes.get(i), uSize.get(i)));
            writer.append("\n");
        }
    }

}

package se.kth.experiments;

import se.kth.edge.CacheEntry;
import se.kth.edge.CacheManager;
import se.kth.stream.StreamFileReader;
import se.kth.stream.Tuple;

import java.io.*;
import java.util.*;

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
    private final static CacheManager eManager = new CacheManager(window, CacheManager.SizePolicy.EAGER, CacheManager.EvictionPolicy.LFU, alpha);
    private final static CacheManager lManager = new CacheManager(window, CacheManager.SizePolicy.LAZY, CacheManager.EvictionPolicy.LFU, alpha);
    private final static CacheManager hManager = new CacheManager(window, CacheManager.SizePolicy.HYBRID, CacheManager.EvictionPolicy.LFU, alpha);

    private final static String inputFile = "/Users/ganymedian/Desktop/aggregation/synthdataset.txt";
    private final static String eOutputFile = "/Users/ganymedian/Desktop/aggregation/eoutput.txt";
    private final static String lOutputFile = "/Users/ganymedian/Desktop/aggregation/loutput.txt";
    private final static String hOutputFile = "/Users/ganymedian/Desktop/aggregation/houtput.txt";
    private final static String oOutputFile = "/Users/ganymedian/Desktop/aggregation/ooutput.txt";
    private static PrintWriter eWriter;
    private static PrintWriter lWriter;
    private static PrintWriter hWriter;
    private static PrintWriter oWriter;


    public static void main(String[] args) throws IOException {

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

    private static void timestepExecution(LinkedList<Tuple> tuples, int timestep) throws IOException {

        int windowCounter = 0;

        long time = 0; //TODO create time step
        boolean windowStarts = true;
        while (true) {
            if (windowStarts) {
                resetOnWindowStart();
                windowStarts = false;
            }

            //If timestep
            List<CacheEntry> eUpdates = eManager.trigger(time, windowCounter * window, avgBw);
            List<CacheEntry> lUpdates = lManager.trigger(time, windowCounter * window, avgBw);
            List<CacheEntry> hUpdates = hManager.trigger(time, windowCounter * window, avgBw);
            triggerTimes.add(time);

            int eSize = eManager.getCurrentCacheSize();
            eCacheSizes.add(eSize);
            int lSize = lManager.getCurrentCacheSize();
            lCacheSizes.add(lSize);
            int hSize = hManager.getCurrentCacheSize();
            hCacheSizes.add(hSize);

            eUpdateSize.add(eUpdates.size());
            lUpdateSize.add(lUpdates.size());
            hUpdateSize.add(hUpdates.size());

            if (time == ((windowCounter + 1) * window)) // end of the window
            {
                // Last tuple in the current window

                // Jump to the end of the window
                // Set for a new window.
                windowStarts = true;
                OptimalCacheStatistics statistics = new OptimalCacheStatistics(tuplesPerWindow, time - window, timestep, window);
                System.out.println(String.format("End of w%d", windowCounter));
                System.out.println(String.format("Estimated Eager Cache Sizes: %s", Arrays.toString(eCacheSizes.toArray())));
                System.out.println(String.format("Estimated Lazy Cache Sizes: %s", Arrays.toString(lCacheSizes.toArray())));
                System.out.println(String.format("Estimated Hybrid Cache Sizes: %s", Arrays.toString(hCacheSizes.toArray())));
                System.out.println(String.format("Optimal Cache Sizes: %s", Arrays.toString(statistics.getCacheSizes().toArray())));
                writeToFile(triggerTimes, windowCounter + 1, eCacheSizes, eUpdateSize, eWriter);
                writeToFile(triggerTimes, windowCounter + 1, lCacheSizes, lUpdateSize, lWriter);
                writeToFile(triggerTimes, windowCounter + 1, hCacheSizes, hUpdateSize, hWriter);
                writeToFile(triggerTimes, windowCounter + 1, statistics.getCacheSizes(), statistics.getUpdateSizes(), oWriter);
                windowCounter++;
                if (tuples.isEmpty()) {
                    break;
                }
            } else {

                time += timestep;

                while (tuples.peek() != null && (tuples.peek().getTimestamp() < time)) {
                    Tuple t = tuples.poll();
                    tuplesPerWindow.add(t);
                    eManager.insert(t.getKey(), t.getTimestamp());
                    lManager.insert(t.getKey(), t.getTimestamp());
                    hManager.insert(t.getKey(), t.getTimestamp());
                }
            }
        }

    }

    private static void tupleStepExecution(LinkedList<Tuple> tuples) {
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
            eManager.insert(t1.getKey(), t1.getTimestamp());
            lManager.insert(t1.getKey(), t1.getTimestamp());
            hManager.insert(t1.getKey(), t1.getTimestamp());

            //If timestep
            List<CacheEntry> eUpdates = eManager.trigger(t1.getTimestamp(), windowCounter * window, avgBw);
            List<CacheEntry> lUpdates = lManager.trigger(t1.getTimestamp(), windowCounter * window, avgBw);
            List<CacheEntry> hUpdates = hManager.trigger(t1.getTimestamp(), windowCounter * window, avgBw);
            triggerTimes.add(t1.getTimestamp());

            int eSize = eManager.getCurrentCacheSize();
            eCacheSizes.add(eSize);
            int lSize = lManager.getCurrentCacheSize();
            lCacheSizes.add(lSize);
            int hSize = hManager.getCurrentCacheSize();
            hCacheSizes.add(hSize);

            eUpdateSize.add(eUpdates.size());
            lUpdateSize.add(lUpdates.size());
            hUpdateSize.add(hUpdates.size());

            if (t1.getTimestamp() / window != t2.getTimestamp() / window) // t1 is the last arrival in the current window.
            {
                // Last tuple in the current window

                // Jump to the end of the window
                // Set for a new window.

                windowStarts = true;
                OptimalCacheStatistics statistics = new OptimalCacheStatistics(tuplesPerWindow);
                System.out.println(String.format("End of w%d", windowCounter));
                System.out.println(String.format("Estimated Eager Cache Sizes: %s", Arrays.toString(eCacheSizes.toArray())));
                System.out.println(String.format("Estimated Lazy Cache Sizes: %s", Arrays.toString(lCacheSizes.toArray())));
                System.out.println(String.format("Estimated Hybrid Cache Sizes: %s", Arrays.toString(hCacheSizes.toArray())));
                System.out.println(String.format("Optimal Cache Sizes: %s", Arrays.toString(statistics.getCacheSizes().toArray())));
                writeToFile(triggerTimes, windowCounter + 1, eCacheSizes, eUpdateSize, eWriter);
                writeToFile(triggerTimes, windowCounter + 1, lCacheSizes, lUpdateSize, lWriter);
                writeToFile(triggerTimes, windowCounter + 1, hCacheSizes, hUpdateSize, hWriter);
                writeToFile(triggerTimes, windowCounter + 1, statistics.getCacheSizes(), statistics.getUpdateSizes(), oWriter);
                windowCounter++;
            } else {
                windowStarts = false;
            }
        }
    }


    private static void resetOnWindowStart() {
        tuplesPerWindow.clear();
        eManager.nextWindow();
        lManager.nextWindow();
        hManager.nextWindow();
        eCacheSizes.clear();
        lCacheSizes.clear();
        hCacheSizes.clear();
        triggerTimes.clear();
        eUpdateSize.clear();
        lUpdateSize.clear();
        hUpdateSize.clear();
    }

    private static void writeToFile(List<Long> triggerTimes, int window, List<Integer> cSizes, List<Integer> uSize, PrintWriter writer) {
        for (int i = 0; i < triggerTimes.size(); i++) {
            writer.append(String.format("%d,%d,%d,%d", triggerTimes.get(i), window, cSizes.get(i), uSize.get(i)));
            writer.append("\n");
        }
    }

}

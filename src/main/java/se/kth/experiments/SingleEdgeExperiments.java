package se.kth.experiments;

import se.kth.edge.CacheEntry;
import se.kth.edge.CacheManager;
import se.kth.stream.StreamFileReader;
import se.kth.stream.Tuple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by Hooman on 2017-06-11.
 */
public class SingleEdgeExperiments {

    public static void main(String[] args) throws IOException {
        String inputFile = "/Users/ganymedian/Desktop/aggregation/synthdataset.txt";
        String eOutputFile = "/Users/ganymedian/Desktop/aggregation/eoutput.txt";
        String lOutputFile = "/Users/ganymedian/Desktop/aggregation/loutput.txt";
        String hOutputFile = "/Users/ganymedian/Desktop/aggregation/houtput.txt";
        String oOutputFile = "/Users/ganymedian/Desktop/aggregation/ooutput.txt";
        PrintWriter eWriter = new PrintWriter(new FileOutputStream(new File(eOutputFile)));
        PrintWriter lWriter = new PrintWriter(new FileOutputStream(new File(lOutputFile)));
        PrintWriter hWriter = new PrintWriter(new FileOutputStream(new File(hOutputFile)));
        PrintWriter oWriter = new PrintWriter(new FileOutputStream(new File(oOutputFile)));
        String header = "timestamp,window,cachesize,updatesize\n";
        eWriter.write(header);
        lWriter.write(header);
        hWriter.write(header);
        oWriter.write(header);
        LinkedList<Tuple> tuples = StreamFileReader.read(inputFile);
        LinkedList<Long> triggerTimes = new LinkedList<>();
        System.out.println(String.format("Number of tuples: %d", tuples.size()));
        float alpha = 0.25f;
        float avgBw = 100;

        int window = 100;
        int windowCounter = 0;

        boolean windowStarts = true;

        LinkedList<Tuple> tuplesPerWindow = new LinkedList<>();
        LinkedList<Integer> eCacheSizes = new LinkedList<>();
        LinkedList<Integer> lCacheSizes = new LinkedList<>();
        LinkedList<Integer> hCacheSizes = new LinkedList<>();
        LinkedList<Integer> eUpdateSize = new LinkedList<>();
        LinkedList<Integer> lUpdateSize = new LinkedList<>();
        LinkedList<Integer> hUpdateSize = new LinkedList<>();

        CacheManager eManager = new CacheManager(window, CacheManager.SizePolicy.EAGER, CacheManager.EvictionPolicy.LFU, alpha);
        CacheManager lManager = new CacheManager(window, CacheManager.SizePolicy.LAZY, CacheManager.EvictionPolicy.LFU, alpha);
        CacheManager hManager = new CacheManager(window, CacheManager.SizePolicy.HYBRID, CacheManager.EvictionPolicy.LFU, alpha);

        for (int i = 0; i < tuples.size() - 1; i++) {
            if (windowStarts) {
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

            Tuple t1 = tuples.get(i); // current event
            Tuple t2 = tuples.get(i + 1); // next event
            tuplesPerWindow.add(t1);
            eManager.insert(t1.getKey(), t1.getTimestamp());
            lManager.insert(t1.getKey(), t1.getTimestamp());
            hManager.insert(t1.getKey(), t1.getTimestamp());

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

        eWriter.flush();
        lWriter.flush();
        hWriter.flush();
        oWriter.flush();
        eWriter.close();
        lWriter.close();
        hWriter.close();
        oWriter.close();

    }

    private static void writeToFile(List<Long> triggerTimes, int window, List<Integer> cSizes, List<Integer> uSize, PrintWriter writer) {
        for (int i = 0; i < triggerTimes.size(); i++) {
            writer.append(String.format("%d,%d,%d,%d", triggerTimes.get(i), window, cSizes.get(i), uSize.get(i)));
            writer.append("\n");
        }
    }

}

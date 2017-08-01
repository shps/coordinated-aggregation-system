package se.kth.stream.satori;

import com.google.gson.JsonObject;
import com.satori.rtm.*;
import com.satori.rtm.model.*;
import se.kth.stream.Tuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

/**
 * Created by Hooman on 2017-07-26.
 */
public class TweetCollector {

    static final String endpoint = "wss://open-data.api.satori.com";
    static final String appkey = "681f26c7c7fAFde3DfcAe9AE7F4e8d4d";
    static final String channel = "Twitter-statuses-sample";
    static String outputFile = "/Users/ganymedian/Desktop/aggregation/twitter.txt";

    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        final RtmClient client = new RtmClientBuilder(endpoint, appkey)
                .setListener(new RtmClientAdapter() {
                    @Override
                    public void onEnterConnected(RtmClient client) {
                        System.out.println("Connected to Satori RTM!");
                    }
                })
                .build();

        PrintWriter writer = new PrintWriter(new FileOutputStream(new File(outputFile)));

        SubscriptionAdapter listener = new SubscriptionAdapter() {
            int counter = 0;
            @Override
            public void onSubscriptionData(SubscriptionData data) {
                for (AnyJson json : data.getMessages()) {
                    counter++;
                    JsonObject o = json.convertToType(JsonObject.class);
                    if (o.get("delete") != null)
                        continue;
//                    System.out.println("Got message: " + json.toString());
                    writer.append(json.toString()).append("\n");
                    writer.flush();
                    System.out.println(counter);
                }
            }
        };

        client.createSubscription(channel, SubscriptionMode.SIMPLE, listener);

        client.start();
    }
}

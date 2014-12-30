package org.p7h.storm.hashtags.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Counts the hashtags and displays the count info to the console and also logs to the file.
 *
 * @author - Prashanth Babu
 */
public final class HashtagCountBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(HashtagCountBolt.class);
    private static final long serialVersionUID = 2884290199245000383L;

    /**
     * Interval between logging the output.
     */
    private final long logIntervalInSeconds;
    /**
     * Log only the top 10 hashtags.
     */
    private final int rankMaxThreshold;

    private Multiset<String> hashtagsTrackerMultiset;
    private Multimap<Integer, String> frequencyOfHashtags;
    private long runCounter;
    private Stopwatch stopwatch = null;

    public HashtagCountBolt(final long logIntervalInSeconds, final int rankMaxThreshold) {
        this.logIntervalInSeconds = logIntervalInSeconds;
        this.rankMaxThreshold = rankMaxThreshold;
    }

    @Override
    public final void prepare(final Map map, final TopologyContext topologyContext,
                              final OutputCollector collector) {
        this.hashtagsTrackerMultiset = HashMultiset.create();
        //Doing this circus so that the output is in a proper descending order of the count of words.
        this.frequencyOfHashtags = Multimaps.newListMultimap(
                new TreeMap<>(Ordering.natural().reverse()), () -> Lists.newArrayList()
        );
        runCounter = 0;
        stopwatch = Stopwatch.createStarted();
    }

    @Override
    public final void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void execute(final Tuple input) {
        final List<String> hashtags = (List<String>) input.getValueByField("hashtags");
        hashtagsTrackerMultiset.addAll(hashtags);

        if (logIntervalInSeconds <= stopwatch.elapsed(TimeUnit.SECONDS)) {
            logHashtagCount();
            stopwatch.reset();
            stopwatch.start();
        }
    }

    private final void logHashtagCount() {
        //We would like to get the count of hashtags and its corresponding hashtags.
        //Group hashtags based on the count into a Multimap.
        for (String type : Multisets.copyHighestCountFirst(hashtagsTrackerMultiset).elementSet()) {
            this.frequencyOfHashtags.put(this.hashtagsTrackerMultiset.count(type), type);
        }
        final StringBuilder dumpHashtagsToLog = new StringBuilder();

        List<String> hashtags;
        int iLoopcounter = 0;
        String keyString;
        int maxLength = 0;
        int keyStringLength = 0;
        for (final int key : this.frequencyOfHashtags.keySet()) {
            //Reduce the unnecessary noise by removing hashtags which appeared less than twice.
            if (2 >= key) {
                continue;
            }
            hashtags = (List<String>) this.frequencyOfHashtags.get(key);
            Collections.sort(hashtags);
            //For adding proper padding at the start.
            keyString = String.valueOf(key);
            keyStringLength = keyString.length();
            maxLength = (maxLength == 0 ? keyStringLength : maxLength);
            //Write to console and / or log file.
            dumpHashtagsToLog
                    .append("\t")
                    .append(Strings.padStart(keyString, (maxLength - keyStringLength), ' '))
                    .append(" ==> ")
                    .append(hashtags)
                    .append("\n");
            iLoopcounter++;
            if (iLoopcounter > rankMaxThreshold) {
                break;
            }
        }
        this.runCounter++;
        LOGGER.info("At {}, total # of hashtags received in run#{}: {} ", new Date(), runCounter,
                hashtagsTrackerMultiset.size());
        LOGGER.info("\n{}", dumpHashtagsToLog.toString());

        // Empty frequency and tracker Maps for further iterations.
        this.hashtagsTrackerMultiset.clear();
        this.frequencyOfHashtags.clear();
    }
}
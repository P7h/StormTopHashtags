package org.p7h.storm.hashtags.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.p7h.storm.hashtags.bolts.HashtagCountBolt;
import org.p7h.storm.hashtags.spouts.TwitterSpout;
import org.p7h.storm.hashtags.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the elements and forms a Topology to find the top hashtags.
 *
 * @author - Prashanth Babu
 */
public final class TopHashtagsTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(TopHashtagsTopology.class);

	public static final void main(final String[] args) throws Exception {
		try {
			final Config config = new Config();
			config.setMessageTimeoutSecs(120);
			config.setDebug(false);

			final TopologyBuilder topologyBuilder = new TopologyBuilder();
			topologyBuilder.setSpout("twitterspout", new TwitterSpout());
			//Create Bolt with the frequency of logging [in seconds] and # of sorted [descending order] hashtags to log.
			topologyBuilder.setBolt("hashtagcountbolt", new HashtagCountBolt(30, 10))
					.shuffleGrouping("twitterspout");

			//Submit it to the cluster, or submit it locally
			if (null != args && 0 < args.length) {
				config.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
			} else {
				config.setMaxTaskParallelism(10);
				final LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology(Constants.TOPOLOGY_NAME, config, topologyBuilder.createTopology());
				//Run this topology for 120 seconds so that we can complete processing of decent # of tweets.
				Utils.sleep(120 * 1000);

				LOGGER.info("Shutting down the cluster...");
				localCluster.killTopology(Constants.TOPOLOGY_NAME);
				localCluster.shutdown();
			}
		} catch (final AlreadyAliveException | InvalidTopologyException exception) {
			//Deliberate no op;
			exception.printStackTrace();
		} catch (final Exception exception) {
			//Deliberate no op;
			exception.printStackTrace();
		}
		LOGGER.info("\n\n\n\t\t*****Please clean your temp folder \"{}\" now!!!*****", System.getProperty("java.io.tmpdir"));
	}
}
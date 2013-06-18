package org.p7h.storm.hashtags.spouts;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.collect.Lists;
import org.p7h.storm.hashtags.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Spout which gets tweets from Twitter using OAuth Credentials.
 *
 * @author - Prashanth Babu
 */
public final class TwitterSpout extends BaseRichSpout {
	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterSpout.class);
	private static final long serialVersionUID = -1506848123321622185L;

	private SpoutOutputCollector _collector;
    private LinkedBlockingQueue<Status> _queue;
    private TwitterStream _twitterStream;

	@Override
	public final void open(final Map conf, final TopologyContext context,
	                 final SpoutOutputCollector collector) {
		this._queue = new LinkedBlockingQueue<>(1000);
		this._collector = collector;

		final StatusListener statusListener = new StatusListener() {
			@Override
			public void onStatus(final Status status) {
				_queue.offer(status);
			}

			@Override
			public void onDeletionNotice(final StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(final int i) {
			}

			@Override
			public void onScrubGeo(final long l, final long l1) {
			}

			@Override
			public void onStallWarning(final StallWarning stallWarning) {
			}

			@Override
			public void onException(final Exception e) {
			}
		};
		//Twitter stream authentication setup
		final Properties properties = new Properties();
		try {
			properties.load(TwitterSpout.class.getClassLoader()
					                .getResourceAsStream(Constants.CONFIG_PROPERTIES_FILE));
		} catch (final IOException exception) {
			//Should not occur. If it does, we cant continue. So exiting the program!
			LOGGER.error(exception.toString());
			System.exit(1);
		}

		final ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
		configurationBuilder.setIncludeEntitiesEnabled(true);

		configurationBuilder.setOAuthAccessToken(properties.getProperty(Constants.OAUTH_ACCESS_TOKEN));
		configurationBuilder.setOAuthAccessTokenSecret(properties.getProperty(Constants.OAUTH_ACCESS_TOKEN_SECRET));
		configurationBuilder.setOAuthConsumerKey(properties.getProperty(Constants.OAUTH_CONSUMER_KEY));
		configurationBuilder.setOAuthConsumerSecret(properties.getProperty(Constants.OAUTH_CONSUMER_SECRET));
		this._twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
		this._twitterStream.addListener(statusListener);

		//Returns a small random sample of all public statuses.
		this._twitterStream.sample();
	}

	@Override
	public final void nextTuple() {
		final Status status = _queue.poll();
		if (null == status) {
			//If _queue is empty sleep the spout thread so it doesn't consume resources.
			Utils.sleep(500);
        } else {
			HashtagEntity[] hashtagEntities =  status.getHashtagEntities();
			List<String> hashtags = Lists.newArrayList();
			for(HashtagEntity hashtagEntity : hashtagEntities){
				hashtags.add(hashtagEntity.getText().toLowerCase());
			}
			this._collector.emit(new Values(hashtags));
		}
	}

	@Override
	public final void close() {
		this._twitterStream.shutdown();
	}

	@Override
	public final void ack(final Object id) {
	}

	@Override
	public final void fail(final Object id) {
	}

	@Override
	public final void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtags"));
	}
}
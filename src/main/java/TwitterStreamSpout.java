
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import utils.TwitterClient;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreamSpout extends BaseRichSpout
{
    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector)
    {
        this.collector = spoutOutputCollector;
        queue = new LinkedBlockingQueue<>(1000);

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                if (status.getUser().getLocation() != null)
                    if (status.getPlace().getCountry().equalsIgnoreCase("India"))
                        queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("onDeletionNotice");
            }

            @Override
            public void onTrackLimitationNotice(int i) {
                System.out.println("onTrackLimitationNotice");
            }

            @Override
            public void onScrubGeo(long l, long l1) {
                System.out.println("onScrubGeo");
            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {
                System.out.println("onStallWarning");
            }

            @Override
            public void onException(Exception e) {
                e.printStackTrace();
            }
        };

        try
        {
            TwitterClient twitterClient = new TwitterClient();

            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true);
            cb.setOAuthConsumerKey(twitterClient.getConsumerKey());
            cb.setOAuthConsumerSecret(twitterClient.getConsumerSecret());
            cb.setOAuthAccessToken(twitterClient.getAccessToken());
            cb.setOAuthAccessTokenSecret(twitterClient.getAccessTokenSecret());
            cb.setJSONStoreEnabled(true);
            twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
            twitterStream.addListener(listener);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple()
    {
        Status status = queue.poll();

        try
        {
            if (status == null)
                Thread.sleep(100);
            else
                collector.emit(new Values(status));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    @Override
    public void close()
    {
        twitterStream.shutdown();
    }
}

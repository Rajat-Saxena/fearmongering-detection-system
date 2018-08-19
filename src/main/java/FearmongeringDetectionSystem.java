import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import utils.Constants;
import utils.GetPanicWords;

public class FearmongeringDetectionSystem
{
    public static void main(String[] args) throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(Constants.twitterStreamSpoutId, new TwitterStreamSpout(), 5);

        builder.setBolt(Constants.filterPanicTweetBoltId, new FilterPanicTweetsBolt(), 8)
                .shuffleGrouping(Constants.twitterStreamSpoutId);

        builder.setBolt(Constants.filterReliableTweetsBoltId, new FilterReliableTweetsBolt(), 8)
                .shuffleGrouping(Constants.twitterStreamSpoutId);

        // Get panic words, parse and store in list.
        new GetPanicWords();

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(Constants.topologyName, conf, builder.createTopology());

        Thread.sleep(10000);
        cluster.shutdown();
    }
}

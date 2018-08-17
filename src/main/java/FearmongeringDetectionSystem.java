import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import utils.Constants;

public class FearmongeringDetectionSystem
{
    public static void main(String[] args) throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(Constants.twitterStreamSpoutId, new TwitterStreamSpout(), 5);

        builder.setBolt(Constants.identifyPanicTweetBoltId, new IdentifyPanicTweetBolt(), 8)
                .shuffleGrouping(Constants.twitterStreamSpoutId);

        builder.setSpout(Constants.twitterReliableNewsSpoutId, new TwitterReliableNewsSpout(), 5);

        builder.setBolt(Constants.filterReliableTweetsBoltId, new FilterReliableTweetsBolt(), 8)
                .fieldsGrouping(Constants.twitterReliableNewsSpoutId, new Fields(""));

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(Constants.topologyName, conf, builder.createTopology());

        Thread.sleep(10000);
        cluster.shutdown();
    }

    /*public void getPanicWords()
    {
        String s = readUrl("http://api.datamuse.com/words?rd=*");
    }*/
}

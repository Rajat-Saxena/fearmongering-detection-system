import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;
import utils.GetPanicWords;

import java.util.Map;

public class FilterPanicTweetsBolt extends BaseRichBolt
{
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector)
    {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        Status tweet = (Status) tuple.getValueByField("tweet");

        String text = tweet.getText().replaceAll("\\p{Punct}", " ");
        String[] words = text.split(" ");

        for (String word : words)
        {
            if (GetPanicWords.PANIC_WORDS_LIST.contains(word))
            {
                System.out.println("\n# Panic tweet:\n" + tweet.getText());
                collector.emit(new Values(tweet));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        outputFieldsDeclarer.declare(new Fields("panic-tweet"));
    }
}

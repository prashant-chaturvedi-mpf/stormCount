package org.apache.maven.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CountBolt extends BaseRichBolt{
	
	private OutputCollector ocollector;
	private HashMap<String, Long> counts = null;
	
	//To establish the input stream for the current bolt
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.ocollector = collector;
		this.counts = new HashMap<String, Long>();
	}
	
	//To process the actual logic on the input Tuple provided ;to get the word count
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = this.counts.get(word);
		if(count == null){
			count = 0L;
		}
		count++;
		this.counts.put(word, count);
		this.ocollector.emit(new Values(word, count));
	}
	
	//To declare the output fields which are released from this bolt
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
}

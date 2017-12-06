package org.apache.maven.storm.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class SplitBolt extends BaseRichBolt{
	
	private OutputCollector ocollector;
	

	//To establish the input stream for the current bolt
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.ocollector = collector;
	}
	
	//To process the actual logic on the input Tuple provided ; here splitting the complete statements into words
	public void execute(Tuple tuple) {
		String statement = tuple.getStringByField("statement");
		String[] words = statement.split(" ");
		for(String word : words){
			this.ocollector.emit(new Values(word));
		}
	}
	
	//To declare the output fields which are released from this bolt
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}
package org.apache.maven.storm.bolt;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class DisplayBolt extends BaseRichBolt {
	
	private HashMap<String, Long> counts = null;
	
	//To establish the input stream for the current bolt a Map of word and count
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.counts = new HashMap<String, Long>();
	}
	
	//To process the actual logic on the input Tuple provided ; 
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = tuple.getLongByField("count");
		this.counts.put(word, count);
	}

	//This is just done to overcome unimplemented method of extended class
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	//Cleanup for displaying the output of the complete topology
	@Override
	public void cleanup() {
		List<String> keys = new ArrayList<String>();
		keys.addAll(this.counts.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			System.out.println(key + " : " + this.counts.get(key));
		}
		System.out.println("------END--------");
	}
}

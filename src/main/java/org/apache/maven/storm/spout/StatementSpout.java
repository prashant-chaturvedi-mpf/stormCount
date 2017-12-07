package org.apache.maven.storm.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

@SuppressWarnings("serial")
public class StatementSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private String[] statement = {
			"The villagers of Little Hangleton still called it The Riddle House.",

			"Even though it had been many years since the Riddle family had lived there.",

			"It stood on a hill overlooking the village.",

			"Some of its windows boarded, tiles missing from its roof, and ivy spreading unchecked over its face.",

			"Once a fine-looking manor, and easily the largest and grandest building for miles around.",

			"The Riddle House was now damp, derelict, and unoccupied." };

	private int snumber = 0;

	// To declare the tuple which will be output from StatementSpout
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("statement"));
	}

	// This method is used for init of topology
	public void open(Map config, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	// For iterating different tuples to the topology for processing , picking
	// up one statement indexed one at a time
	public void nextTuple() {
		Utils.sleep(1000);
		this.collector.emit(new Values(statement[snumber]));
		snumber++;
		if (snumber >= statement.length) {
			snumber = 0;
		}

	}

}

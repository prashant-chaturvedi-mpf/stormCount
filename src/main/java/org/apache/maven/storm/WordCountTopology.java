package org.apache.maven.storm;


import org.apache.maven.storm.bolt.CountBolt;
import org.apache.maven.storm.bolt.DisplayBolt;
import org.apache.maven.storm.bolt.SplitBolt;
import org.apache.maven.storm.spout.StatementSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class WordCountTopology {
	
	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String DISPLAY_BOLT_ID = "display-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";
	
	public static void main(String[] args) throws Exception {
		
		StatementSpout spout = new StatementSpout();
		SplitBolt splitBolt = new SplitBolt();
		CountBolt countBolt = new CountBolt();
		DisplayBolt displayBolt = new DisplayBolt();
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(SENTENCE_SPOUT_ID, spout);
		// StatementSpout to SplitBolt
		builder.setBolt(SPLIT_BOLT_ID, splitBolt)
		.shuffleGrouping(SENTENCE_SPOUT_ID);
		
		// SplitBolt to CountBolt
		builder.setBolt(COUNT_BOLT_ID, countBolt)
		.fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
		
		// CountBolt to DisplayBolt
		builder.setBolt(DISPLAY_BOLT_ID, displayBolt)
		.globalGrouping(COUNT_BOLT_ID);
		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		Utils.sleep(20000);
		cluster.killTopology(TOPOLOGY_NAME);
		//Utils.sleep(10000);
		System.out.println("before");
		cluster.shutdown();
		System.out.println("after");
	}
}
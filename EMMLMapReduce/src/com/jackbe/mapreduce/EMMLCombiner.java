package com.jackbe.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;


/**
 * The Combine class acts as an intermediary reducer. It will receive all the 
 * mapper key/values for a given key and can further combine (reduce the number)
 * before sending off to the reducer so that the reducer is overwhelmed.
 * 
 * @author Christopher Steel - JackBe
 *
 * @since Jul 13, 2011 10:07:24 AM
 * @version 1.0
 */
public class EMMLCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	static Logger log = Logger.getLogger(EMMLCombiner.class);
	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		log.info("Combine.reduce called for key:" + key);
		
		EMMLMapReduce mapReduce;
		try {
			mapReduce = EMMLMapReduce.getInstance();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
		mapReduce.executeScript(mapReduce.combinerScript, key, mapReduce.getValues(values), output, EMMLMapReduce.COMBINER_SCRIPT);

	}  //reduce
	
} //class Combine

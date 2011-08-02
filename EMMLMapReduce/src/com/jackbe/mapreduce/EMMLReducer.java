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
 * The reduce method in the Reduce class is called for each set of keys from the 
 * Combiner. In this case it is called for every symboldate pair. The reduce 
 * method will then calculate the Daily moving average for each symbol.
 *   
 * @author Christopher Steel - FortMoon Consulting
 *
 * @since Jul 13, 2011 10:24:21 AM
 * @version 1.0
 */
public class EMMLReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	static Logger log = Logger.getLogger(EMMLReducer.class);
	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		EMMLMapReduce mapReduce;
		log.debug("called.");
		try {
			mapReduce = EMMLMapReduce.getInstance();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
//		mapReduce.executeScript(mapReduce.reducerScript, key, mapReduce.getValues(values), output);
		while(values.hasNext()) {
			String value = values.next().toString();
			if(log.isInfoEnabled())
				log.info("Value going in to reduce script: \n" + value);
			mapReduce.executeScript(mapReduce.reducerScript, key, value, output, EMMLMapReduce.REDUCER_SCRIPT);
		}
	} //reduce

} // classe Reduce

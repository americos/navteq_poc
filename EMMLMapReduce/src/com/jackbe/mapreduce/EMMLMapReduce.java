package com.jackbe.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.log4j.Logger;
import org.oma.emml.me.runtime.EMMLService;
import org.oma.emml.me.runtime.EMMLServiceFactory;

import com.jackbe.mapreduce.examples.stock.StockExampleEMML;

@SuppressWarnings("deprecation")
public class EMMLMapReduce {
	protected String emml = null;
	protected String mapperScript;
	protected String reducerScript;
	protected String combinerScript;
	protected JobClient jobClient = null;
	protected JobConf conf = new JobConf(EMMLMapReduce.class);
	private static EMMLMapReduce instance = null;
	protected EMMLService emmlService = null;
	HashMap<String, RunningJob> statusMap = new HashMap<String, RunningJob>();
	public static int MAPPER_SCRIPT = 0;
	public static int COMBINER_SCRIPT = 1;
	public static int REDUCER_SCRIPT = 2;
	protected static String MAP_REDUCE_KEY_BEGIN_TAG = "<MapReduceKey>";
	protected static String MAP_REDUCE_KEY_END_TAG = "</MapReduceKey>";

	private static long counter = 0;
	static Logger log = Logger.getLogger(EMMLMapReduce.class);

	protected EMMLMapReduce() throws Exception {
		super();
		jobClient = new JobClient(conf);
		emmlService = EMMLServiceFactory.createService();
	}

	/**
	 * Method for retrieving the Singleton instance.
	 *
	 * @return Singleton instance
	 */
	public static EMMLMapReduce getInstance() {
		if(instance == null) {
			try {
				instance = new EMMLMapReduce();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return instance;
	}

	/**
	 * This method returns the overall progress of a RunningJob. It retrieves the map progress
	 * and the reduce progress and returns the sum of those results / 2.
	 * 
	 * @param job
	 * @return the progress of the map and reduce tasks of a RunningJob as a number between 0.0 and 1.0
	 */
	public float getJobProgress(RunningJob job) {
		if(job == null) {
			return 0.0f;
		}
		try {
			if (job.mapProgress() == 0.0f) 
				return 0.0f;

			//log.debug(": " + conf.getNumMapTasks() + ":" + conf.getNumReduceTasks() + ":"+ conf.getNumTasksToExecutePerJvm());
			return ((job.mapProgress() + job.reduceProgress())/2.0f)*100.0f;
		} 
		catch (IOException e) {
			log.error("Exception getting job progress: " + e);
			return 0.0f;
		}

	}

	/**
	 * Utility method for retrieving Job progress using a String id instead of the 
	 * RunningJob.
	 * 
	 * @param id The RunningJon string identifier
	 * @return
	 */
	public float getJobProgress(String id) {
		RunningJob job = null;
		try {
			job = jobClient.getJob(JobID.forName(id));
		} catch (Exception e) {
			e.printStackTrace();
			return 0.0f;
		}
		return getJobProgress(job);
	}

	public String getJobState(RunningJob job) throws IOException {
		if(job == null)
			return "Job is null";

		return JobStatus.getJobRunState(job.getJobState());
	}

	public String getJobState(String id) throws IOException {
		RunningJob job = null;
		try {
			job = jobClient.getJob(JobID.forName(id));
		} catch (Exception e) {
			e.printStackTrace();
			return "JOB NOT FOUND";
		}
		return getJobState(job);
	}

	public String getAllJobs() {
		RunningJob[] jobsInfo = null;
		try {
			if(jobClient == null)
				return new String("EXCEPTION: NULL jobClient.");
			//FIXME need to fix for remote jobs
			//jobsInfo = jobClient.getAllJobs();
		} catch (Exception e) {
			e.printStackTrace();
			return new String("EXCEPTION getting jobs: " + e.getMessage());
		}
		// If this is running local, we need to use the Jobs in our map.
		// TODO: remove old entries eventually.

		if(jobsInfo == null ) {
			RunningJob[] temp = new RunningJob[statusMap.size()];
			jobsInfo = statusMap.values().toArray(temp);
		}

		StringBuffer xml = new StringBuffer();
		xml.append("<jobs>\n");
		for(RunningJob job : jobsInfo) {
			try {
				xml.append("<job>\n\t<id>" + job.getID().toString() + "</id>\n\t<state>" + JobStatus.getJobRunState(job.getJobState()));
			} catch (IOException e) {
				log.error("Exception apending job status info: " + e);
			}
			xml.append("</state>\n\t<progress>" + getJobProgress(job.getID().toString()) + "</progress>\n</job>\n");
		}
		xml.append("\n</jobs>");
		return xml.toString();
	}

	public void clearCompletedJobs() {
		for(String key : statusMap.keySet()) {
			RunningJob job = statusMap.get(key);
			try {
				if(job.getJobState() != JobStatus.RUNNING)
					statusMap.remove(key);
			} catch (IOException e) {
				log.error("Exception clearing completed jobs: " + e);
			}
		}
	}

	/**
	 * This method is a utility for returning and XML block containing all values passed in
	 * from the Iterator. The Iterator returns Text values which are converted to Strings
	 * And appended together in the form <value>stringval</value>.
	 * 
	 * @param values A Text Iterator containing the Text values for a given key.
	 * @return String of XML formatted String values.
	 */
	protected String getValues(Iterator<Text> values) {
		if(log.isDebugEnabled())
			log.debug("getValues called.");

		StringBuffer valXml = new StringBuffer();
		while(values.hasNext()) {
			String value = values.next().toString();
//			valXml.append("<value>" + value + "</value>\n");
			if(!value.equals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"))
				valXml.append(value + "\n");
		}
		if(log.isDebugEnabled())
			log.debug("getValues returning: " + valXml.toString());

		return valXml.toString();
	}

	/**
	 * This method excecutes the EMML script for a particular step (map, combine, or reduce).
	 * When a step is called (such as reduce in the Reduce class), it passes this method the
	 * EMML script for the reduce step (reducerScript), the key it was passed, and the values for that
	 * key (along with the output collector and the output key). It puts the key and the value string
	 * in a hashmap and passes it as the input parameters to the EMML script.
	 * <p>
	 * @param script The EMML script to call
	 * @param key The key that references these values as specified by the former collector
	 * @param values The values emitted from the former collector or from the splitter
	 * @param output The output collector which will pass the key/values on to the next collector.
	 */
	protected void executeScript(String script, Text key, String values, 
			OutputCollector<Text, Text> output, int scriptNum) {

		try {
			HashMap<String, String> inputParams = new HashMap<String, String>();
			inputParams.put("key", key.toString());
			inputParams.put("value", values);

			if(log.isDebugEnabled())
				log.debug("Executing script with key: " + key + " and values: \n----------------------------------\n" + values + "\n---------------------------------------------");

//			MashupResponse mashupResponse = emmlService.executeMashupScript(new ExecutionContext(), script, inputParams);
//			EmmlVariable result = mashupResponse.getResult();
//			String resultData = result.getDataAsString();

			HttpClient client = new HttpClient();
			client.getState().setCredentials( new AuthScope("localhost", 8080),
				new UsernamePasswordCredentials("admin", "adminadmin"));

			HttpClientParams params = new HttpClientParams();
			params.setParameter(HttpClientParams.ALLOW_CIRCULAR_REDIRECTS, Boolean.TRUE);
			client.setParams(params);
			client.getParams().setAuthenticationPreemptive(true);
			String value = URLEncoder.encode(values);

			HttpMethod httpMethod = new GetMethod("http://localhost:8080/presto/edge/api/rest/StockQuoteMapper/runMashup?x-presto-resultFormat=xml&value="+value);

			try {
				client.executeMethod(httpMethod);

				log.debug("Status code: " + httpMethod.getStatusCode());
				Header contentTypeHeader = httpMethod
						.getResponseHeader("content-type");
				log.debug("Mimetype: " + contentTypeHeader.getValue());
				log.debug("Response: " + httpMethod.getResponseBodyAsString());
			} finally {
				httpMethod.releaseConnection();
			}

			if(httpMethod.getStatusCode() != 200) {
				return;
			}

			String 	resultData = httpMethod.getResponseBodyAsString();

			if(scriptNum == MAPPER_SCRIPT ) {
				if(log.isDebugEnabled())
					log.debug("Mapper script result:\n" + resultData + "\n");
			}
			if(scriptNum == COMBINER_SCRIPT ) {
				if(log.isDebugEnabled())
					log.debug("Combiner script result:\n" + resultData + "\n");
			}
			if(scriptNum == REDUCER_SCRIPT ) {
				if(log.isDebugEnabled())
					log.debug("Reducer script result:\n" + resultData + "\n");
			}

			// Get key from annotation
			String newKey = null;
			if (resultData.indexOf(MAP_REDUCE_KEY_BEGIN_TAG) > -1) {
				try {
					newKey = resultData.substring(
							resultData.indexOf(MAP_REDUCE_KEY_BEGIN_TAG) + 14,
							resultData.indexOf(MAP_REDUCE_KEY_END_TAG));
				} catch (Exception e) {
					if(log.isInfoEnabled())
						log.info("Exception getting key: " + e);
					newKey = Long.toString(counter++);
				}
			} else {
				newKey = Long.toString(counter++);
				//newKey = Long.toString(1);
			}
			if(log.isDebugEnabled()) {
				log.debug("Key from result data = " + newKey + " for script: " + scriptNum);
				log.debug("Calling collect with result date = " + resultData);
			}

			output.collect(new Text(newKey), new Text(resultData));

		} catch (Exception e) {
			log.error("Exception executing script: " + e, e);
			//e.printStackTrace();
		}		
	}	

	public RunningJob start(String inputDir, String outputDir, String mapperScript, String reducerScript) throws Exception {
		if(inputDir == null || outputDir == null || mapperScript == null || reducerScript == null) {
			if(inputDir == null)
				log.error("ERROR: the inputDir parameter is null");
			if(outputDir == null)
				log.error("ERROR: the outputDir parameter is null");
			if(mapperScript == null)
				log.error("ERROR: the mapperScript parameter is null");
			if(reducerScript == null)
				log.error("ERROR: the reducerScript parameter is null");

			return null;
		}
		return startJob(inputDir, outputDir, mapperScript, reducerScript, null); 
	}

	public RunningJob startJob(String inputDir, String outputDir, String mapperScript, String reducerScript, String combinerScript) throws Exception {

		conf = new JobConf(EMMLMapReduce.class);
		jobClient = new JobClient(conf);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(EMMLMapper.class);
		conf.setReducerClass(EMMLReducer.class);
		//conf.setNumTasksToExecutePerJvm(100);
		//conf.setNumMapTasks(50);
		//conf.setNumReduceTasks(50);

		conf.setJobName("EMMLMapReduce" + System.currentTimeMillis());
		conf.setSessionId(Long.toString(System.currentTimeMillis()));

		if(combinerScript != null)
			conf.setCombinerClass(EMMLCombiner.class);

		conf.setInputFormat(TextInputFormat.class);
		//Use our own XML formatter
		conf.setOutputFormat(XmlOutputFormat.class);  	

		FileInputFormat.setInputPaths(conf, new Path(inputDir));
		FileOutputFormat.setOutputPath(conf, new Path(outputDir));

		File outputPath = new File(outputDir); 
		if(outputPath.exists()) {
			for(String filename : outputPath.list()) { 
				File file = new File(outputDir + "/" + filename); 
				log.info("Deleting file: " + file.getPath()); 
				file.delete(); 
			} 
			outputPath.delete();
			log.info("Deleted output directory."); 
		}


		if(mapperScript != null)
			setMapperScript(mapperScript);

		if(reducerScript != null)
			setReducerScript(reducerScript);

		if(combinerScript != null) {
			setCombinerScript(combinerScript);
		}

		RunningJob job = null;
		try {
			job = jobClient.submitJob(conf);
			statusMap.put(job.getJobID(), job);
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return job;
	}

	private void setMapperScript(String string) {
		mapperScript = string;
	}

	private void setCombinerScript(String string) {
		combinerScript = string;
	}

	private void setReducerScript(String string) {
		reducerScript = string;
	}

	/**
	 * The main method is called by Hadoop when the program is run in a Hadoop
	 * cluster and not just in stand-alone mode. Hadoop will copy the runnable jar
	 * file and invoke it, passing any needed comman-line arguments.
	 * 
	 * @param args Command line arguments needed by program. Usage:<p>
	 * java -jar com.jackbe..mapreduce.aws.EMMLMapReduce <inputDir> <outputDir> <mapperScript> <reducerScript> [<combinerScript>]
	 * <p>where <inputDir> is the directory containing the data files to be processed.<p>
	 * <outputDir> is the directory for Hadoop to put the result files.<p>
	 * <mapperScript> is the EMML text that maps a line of input (usually from a CSV file).<p>
	 * <reducerScript> is the EMML text that combines/reduces the mapped output to a file.
	 * [If no additional mapping is required, use the GenericMapper EMML script].<p> 
	 * 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.out.println("Starting EMML MapReduce.");
		RunningJob job = null;
		EMMLMapReduce mapReduce = EMMLMapReduce.getInstance();

		if (args == null || args.length < 2) {
			job = mapReduce.start("./input", "./output", StockExampleEMML.mapScript, StockExampleEMML.redScript);
		} 
		else {
			if(args.length == 2) {
				job = mapReduce.start(args[0], args[1], StockExampleEMML.mapScript, StockExampleEMML.redScript);
			}		
			if(args.length > 2) {
				job = mapReduce.start(args[0], args[1], args[3], args[4]);
			}		
			if(args.length > 4) {
				job = mapReduce.startJob(args[0], args[1], args[3], args[4], args[5]);
			}

		}
		if(log.isInfoEnabled()) {
			log.info("Job ID: " + job.getID().getId());
			log.info("All Job IDs: " + mapReduce.getAllJobs());
		}
	} //main

}
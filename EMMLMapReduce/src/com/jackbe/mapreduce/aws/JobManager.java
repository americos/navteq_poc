package com.jackbe.mapreduce.aws;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * This class is the entry point for submitting a job, checking its status and retrieving the
 * results from S3.
 * <p>

 */
public class JobManager {
    private final String accessKey = "1HG35KQ9DRW9KX7VEQR2";
    private final String secretKey = "j5PIx2RTp9zKTHNBOqRyAqPop9NbEHIKrt+8Yw/m";
    private Properties props = new Properties();
    private String bucketName = "presto-mapreduce-output";
    private AmazonS3 s3 = null;
    
    public JobManager() {
    	s3 = getS3Client();
    	try {
			createBucket();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * Creates a job to run and returns the String id of that job.
     * 
     * @return id of the job.
     */
    public String createJob() {
        String job = "job-" + UUID.randomUUID();
    	return job;
    }
    
    /**
     * Checks the status of the specified job.
     * 
     * @param id Job ID to check
     * @return Status of the job.
     */
    public String checkStatus(String id) {
    	return "running";
    }
    
    /**
     * Retrieve the results from the specified job.
     * 
     * @param id Job ID of the results to retrieve.
     * @return Result of job.
     */
    public Object getResult(String id) {
        /*
         * Download an object - When you download an object, you get all of
         * the object's metadata and a stream from which to read the contents.
         * It's important to read the contents of the stream as quickly as
         * possibly since the data is streamed directly from Amazon S3 and your
         * network connection will remain open until you read all the data or
         * close the input stream.
         *
         * GetObjectRequest also supports several other options, including
         * conditional downloading of objects based on modification times,
         * ETags, and selectively downloading a range of an object.
         */
        System.out.println("Downloading an object");
        S3Object object = getS3Client().getObject(new GetObjectRequest(bucketName, id));
        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
        try {
			displayTextInputStream(object.getObjectContent());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//FIXME: read the InputStream into an Object and return it.
        return object.getObjectContent();
    }
    
    public AmazonS3 getS3Client() {
    	/**
    	 * FIXME: change this from writing and reading the file to use a bytearrayinputstream.
    	 */
        props.put("accessKey", accessKey);
        props.put("secretKey", secretKey);
        try {
			props.store(new FileOutputStream("AwsCredentials.properties"), 
					"Dynamically generated each runtime by JobManager");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //System.out.println("Props: " + props.toString());
        try {
			s3 = new AmazonS3Client(new PropertiesCredentials(
			        new FileInputStream("AwsCredentials.properties")));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return s3;
    }
    
    public void startJob(String id) throws IOException {

        try {

            /*
             * Upload an object to your bucket - You can easily upload a file to
             * S3, or upload directly an InputStream if you know the length of
             * the data in the stream. You can also specify your own metadata
             * when uploading to S3, which allows you set a variety of options
             * like content-type and content-encoding, plus additional metadata
             * specific to your applications.
             */
            System.out.println("Uploading a new object to S3 from a file\n");
            //s3.putObject(new PutObjectRequest(bucketName, id, createSampleFile()));
            s3.putObject(new PutObjectRequest(bucketName, id, getDataFile()));

        } 
        catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } 
        catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        System.out.println("\nstartJob complete.\n");
    }
    
    public List<String> getResultIDs() {
        /*
         * List objects in your bucket by prefix - There are many options for
         * listing the objects in your bucket.  Keep in mind that buckets with
         * many objects might truncate their results when listing their objects,
         * so be sure to check if the returned object listing is truncated, and
         * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
         * additional results.
         */
    	
    	ArrayList<String> list = new ArrayList<String>();
    	System.out.println("Listing objects");
        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                .withBucketName(bucketName));
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
        	list.add(objectSummary.getKey());
            System.out.println(" - " + objectSummary.getKey() + "  " +
                               "(size = " + objectSummary.getSize() + ")");
        }
        return list;
    }

    /**
     * Creates a temporary file with text data to demonstrate uploading a file
     * to Amazon S3
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */
    @SuppressWarnings("unused")
	private static File createSampleFile() throws IOException {
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.close();

        return file;
    }
    
    private static File getDataFile() {
    	return new File("./input/stocks.csv");
    }
    
    public void createBucket() throws Exception {
        /*
         * List the buckets in your account
         */
        System.out.println("Listing buckets");
        for (Bucket bucket : s3.listBuckets()) {
            //System.out.println(" - " + bucket.getName());
        	if(bucket.getName() == this.bucketName)
        		return;
        }
        // Our bucket does not exist, create it.
 
        /*
         * Create a new S3 bucket - Amazon S3 bucket names are globally unique,
         * so once a bucket name has been taken by any user, you can't create
         * another bucket with that same name.
         *
         * You can optionally specify a location for your bucket if you want to
         * keep your data closer to your applications or users.
         */
        System.out.println("Creating bucket " + bucketName + "\n");
        s3.createBucket(bucketName);
     }

    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input
     *            The input stream to display as text.
     *
     * @throws IOException
     */
    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

	@SuppressWarnings("unused")
	private void deleteResult(String id) {
        /*
         * Delete an object - Unless versioning has been turned on for your bucket,
         * there is no way to undelete an object, so use caution when deleting objects.
         */
        System.out.println("Deleting result for job: " + id);
        s3.deleteObject(bucketName, id);		
	}
	
    public static void main(String[] args) {
		try {
			JobManager jm = new JobManager();
			String id = jm.createJob();
			jm.startJob(id);
			jm.checkStatus(id);
			//System.out.println("Last result id: " + jm.getResultIDs().get(jm.getResultIDs().size() - 1));
			//Object o = jm.getResult(id);
			//jm.deleteResult(id);
		} catch (Exception e) {
			System.out.println("Exception in main: " + e);
		}
	}


}

package manager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import common.Base64;
import common.Consts;

public class Main {
	static AmazonEC2	  ec2;
	static AmazonS3		  s3;
	static AmazonSQS	  sqs;

	private static Logger logger = Logger.getLogger("manager.Main");
	private static final String workerUserData = getWorkerUserData();
	private static final String Bucket = Consts.LINKS_BUCKET_NAME;
	private static final String QueueOut = Consts.OUTPUT_QUEUE_NAME;
	private static final String QueueLinks = Consts.WORKER_PDF_REQUEST_QUEUE_NAME;
	private static final String QueueThumbnails = Consts.WORKER_PDF_RESPONSE_QUEUE_NAME;
	private static final String QueueIn = Consts.INPUT_QUEUE_NAME;
	private static boolean hasNodes = false;
	private static boolean hasMessages = false;
	private static List<AppNums> appNums = new ArrayList<AppNums>();

	private static void init() throws Exception {
		AWSCredentials credentials = new PropertiesCredentials(
				Main.class.getResourceAsStream("/AwsCredentials.properties"));

		ec2 = new AmazonEC2Client(credentials);
		s3  = new AmazonS3Client(credentials);
		sqs = new AmazonSQSClient(credentials);

	}

	public static List<StringPair> receiveFromSQS() throws IOException, Exception {
		List<String> msgs = new ArrayList<String>();
		String[] parsedMsg;
		String[] LparsedMsg;
		String[] RparsedMsg;
		String[] bucketInfo = new String[2];
		List<StringPair> retFileInfo = new ArrayList<StringPair>();
		Exception badmsg = new Exception("bad message syntax");
		try {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(QueueOut);
			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
			for(;;) {
				List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				if((messages.size() == 0) && (!hasNodes)) {//queue is empty
					logger.log(Level.INFO, "Queue is empty");
					Thread.sleep(1000);
				}
				else if(messages.size() != 0){
					for (Message message : messages) {	
						msgs.add(message.getBody());
						String messageRecieptHandle = message.getReceiptHandle();
						sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));
						break;
					}
					break;
				}
				else {
					return null;
				}
			}
			//System.out.println(msg);
			for(String msg : msgs) {
				parsedMsg = msg.split(",");
				if(parsedMsg.length == 2) {
					LparsedMsg = parsedMsg[0].split("=");
					RparsedMsg = parsedMsg[1].split("=");
					if(LparsedMsg[0].equals("Bucket")) 
						bucketInfo[0] = LparsedMsg[1];
					else {
						throw badmsg;
					}
					if(RparsedMsg[0].equals("Key")) 
						bucketInfo[1] = RparsedMsg[1];
					else {
						throw badmsg;
					}
				}
				else {
					throw badmsg;
				}
				retFileInfo.add(new StringPair(bucketInfo[0],bucketInfo[1]));
			}

		}
		catch (AmazonServiceException ase) {
			logger.log(Level.SEVERE, "Amazon service error", ase);
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
			"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:        " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:           " + ase.getErrorType());
			System.out.println("Request ID:           " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			logger.log(Level.SEVERE, "Amazon client error", ace);
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
			"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, "Manager interrupted", e);
			e.printStackTrace();
		}

		return retFileInfo;
	}

	public static List<StringPair> downloadFromS3(List<StringPair> msgsInfo) throws IOException {
		int i=0;
		List<StringPair> bucketLink = new ArrayList<StringPair>();
		try {
			for(StringPair bucketInfo : msgsInfo) {
				i=0;
				//s3.createBucket(bucketInfo.getBucket());
				logger.log(Level.INFO, "Download an object from S3");				
				S3Object object = s3.getObject(new GetObjectRequest(bucketInfo.getStringA(), bucketInfo.getStringB()));
				InputStream input = object.getObjectContent();
				BufferedReader reader = new BufferedReader(new InputStreamReader(input));
				while (true) {
					String line = reader.readLine();
					if (line == null) break;
					bucketLink.add(new StringPair(bucketInfo.getStringB(),line));
					i++;
				}
				appNums.add(new AppNums(bucketInfo.getStringB(),i));
			}


		}
		catch (AmazonServiceException ase) {
			logger.log(Level.SEVERE, "Amazon service error", ase);
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:        " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:           " + ase.getErrorType());
			System.out.println("Request ID:           " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			logger.log(Level.SEVERE, "Amazon client error", ace);
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		return bucketLink;
	}

	public static void createAndSendToSQS(List<StringPair> bucketLinks) throws IOException {
		try {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(QueueLinks);
			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			for(StringPair bucketLink : bucketLinks) {
				String outMsg = "App=" + bucketLink.getStringA() + ",Link=" + bucketLink.getStringB();
				SendMessageRequest msg = new SendMessageRequest(myQueueUrl, outMsg);
				sqs.sendMessage(msg);
			}
		}
		catch (AmazonServiceException ase) {
			logger.log(Level.SEVERE, "Amazon service error", ase);
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
			"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:        " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:           " + ase.getErrorType());
			System.out.println("Request ID:           " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			logger.log(Level.SEVERE, "Amazon client error", ace);
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
			"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}
	
	static String replace(String str, String pattern, String replace) {
	    int s = 0;
	    int e = 0;
	    StringBuffer result = new StringBuffer();

	    while ((e = str.indexOf(pattern, s)) >= 0) {
	        result.append(str.substring(s, e));
	        result.append(replace);
	        s = e+pattern.length();
	    }
	    result.append(str.substring(s));
	    return result.toString();
	}
	
	public static String getWorkerUserData()
	{
		try {
			AWSCredentials credentials = new PropertiesCredentials(
					Main.class.getResourceAsStream("/AwsCredentials.properties"));
			
			String accessKey = credentials.getAWSAccessKeyId();
			String secretKey = credentials.getAWSSecretKey();
			
			InputStream workerUserDataInputStream = 
				Main.class.getResourceAsStream("/worker/start_worker.sh");
			BufferedReader br = new BufferedReader(new InputStreamReader(workerUserDataInputStream));
			StringBuilder sb = new StringBuilder();
			String line;
			
			while ((line = br.readLine()) != null) {
				sb.append(line);
				sb.append('\n');
			}
							
			return Base64.encode(replace(replace(sb.toString(), "REPLACED_WITH_ACCESS_KEY", accessKey),
							"REPLACED_WITH_SECRET_KEY", secretKey));
		} catch (IOException e) {
			logger.log(Level.SEVERE, "I/O error reading user data", e);
			System.exit(1);
			return null;
		}	
	}

	public static List<String> createWorkerNodes(int numOfWorkers) throws IOException {
		List<String> instanceID = new ArrayList<String>();
		try {
			// ubuntu-maverick-10.10-amd64-server-20101225 (ami-cef405a7)
			RunInstancesRequest request = new RunInstancesRequest("ami-cef405a7", numOfWorkers, numOfWorkers);
			request.setInstanceType(InstanceType.T1Micro.toString());
			request.setUserData(workerUserData);
			List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();			
			for(Instance instance : instances) {
				logger.log(Level.INFO, "Created worker instance ", instance.getInstanceId());
				instanceID.add(instance.getInstanceId());
			}
		} catch (AmazonServiceException ase) {
			logger.log(Level.SEVERE, "Amazon service error", ase);
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		}
		return instanceID;
	}

	public static void checkSQSAndDeleteNodes(List<String> instanceIDs) throws IOException, Exception {
		try {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(QueueLinks);
			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			if(messages.size() == 0) {//queue is empty
				TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest(instanceIDs);
				ec2.terminateInstances(terminateInstancesRequest);
				hasMessages = false;
				hasNodes = false;
			}
			else {
				Thread.sleep(1000);
			}
		}
		catch (AmazonServiceException ase) {
			logger.log(Level.SEVERE, "Amazon service error", ase);
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
			"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:        " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:           " + ase.getErrorType());
			System.out.println("Request ID:           " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			logger.log(Level.SEVERE, "Amazon client error", ace);
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
			"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void receiveFromSQS2() throws IOException, Exception {
		List<String> msgs = new ArrayList<String>();
		String[] parsedMsg;
		String LparsedMsg;
		String MparsedMsg;
		String RparsedMsg;
		try {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(QueueThumbnails);
			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			if((messages.size() == 0)) {//queue is empty
				logger.log(Level.INFO, "Queue is empty");
				System.out.println("Queue is empty");
				Thread.sleep(1000);
			}
			else if(messages.size() != 0){
				for (Message message : messages) {	
					msgs.add(message.getBody());
					String messageRecieptHandle = message.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));
					break;
				}
			}
			//System.out.println(msg);
			for(String msg : msgs) {
				parsedMsg = msg.split(",");
				if(parsedMsg.length == 3) {
					LparsedMsg = parsedMsg[0]; //pdf link
					MparsedMsg = parsedMsg[1]; //S3 link of thumbnail
					RparsedMsg = parsedMsg[2]; //uuid of application
					for(AppNums app : appNums) {
						if(app.getKey().equals(RparsedMsg)) {
							app.setCurrentNum(app.getCurrentNum()+1);
							app.links.add(new StringPair(LparsedMsg,MparsedMsg));
							break;
						}
					}
				}
				else {
					throw new Exception("bad syntax of message");
				}
			}

		}
		catch (AmazonServiceException ase) {
			logger.log(Level.SEVERE, "Amazon service error", ase);
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
			"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:        " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:           " + ase.getErrorType());
			System.out.println("Request ID:           " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			logger.log(Level.SEVERE, "Amazon client error", ace);
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
			"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static File createOutputFile(ArrayList<StringPair> links,String filename) throws IOException {
		File file = new File(filename);
		try   {
			// Create file 
			FileWriter fstream = new FileWriter(file);
			BufferedWriter out = new BufferedWriter(fstream);
			for(StringPair link : links) {
				out.write(link.getStringA() + "," + link.getStringB() + "/n");
			}			
			out.close();
		}catch (Exception e){//Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}
		return file;
	}

	public static void uploadFileToS3(File file,String uniqueName) throws IOException {
		try {
			System.out.println("Uploading a new object to S3 from a file\n");
			s3.putObject(new PutObjectRequest(Bucket, uniqueName, file));
		}
		catch (AmazonServiceException ase) {
			logger.log(Level.SEVERE, "Amazon service error", ase);
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:        " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:           " + ase.getErrorType());
			System.out.println("Request ID:           " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			logger.log(Level.SEVERE, "Amazon client error", ace);
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}


	public static void createAndSendToSQS2(String appKey,String uniqueKey) throws IOException {
		try {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(QueueIn);
			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			String outMsg = "AppKey=" + appKey +",Bucket=" + Bucket + ",Key=" + uniqueKey;
			SendMessageRequest msg = new SendMessageRequest(myQueueUrl, outMsg);
			sqs.sendMessage(msg);
		}
		catch (AmazonServiceException ase) {
			logger.log(Level.SEVERE, "Amazon service error", ase);
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
			"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:        " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:           " + ase.getErrorType());
			System.out.println("Request ID:           " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			logger.log(Level.SEVERE, "Amazon client error", ace);
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
			"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	public static void main(String[] args) throws Exception {
		List<StringPair> msgsInfo = new ArrayList<StringPair>();
		List<StringPair> bucketLinks = new ArrayList<StringPair>();
		List<String> instanceIDs = new ArrayList<String>();
		int numOfMsgs;
		int numOfWorkers;
		init();
		for(;;) {
			/* receive bucket and key from the queue for every application. The key is the uuid of
			 * the application UUID. */
			msgsInfo = receiveFromSQS();
			/* if msgInfo is null, meaning that no messages were in the SQS meaning that there was no jobs given by the application,
			 * then you can just jump over the processing of the messages */
			if(msgsInfo != null) {
				/* I download from S3 the pdf list using the bucket and key.
				 * I receive in bucketLinks a pair of strings: <app-key,pdf-link>
				 * I add to the array of appNums objects, for each application, the amount of links needed to process */
				bucketLinks = downloadFromS3(msgsInfo);
				/* I create a queue QueueLinks and send a message for every link, 
				 * containing the UUID/Key of the application and the link itself
				 * Message Format to Worker: App=<UUID OF APP>,Link=<PDF LINK> */
				createAndSendToSQS(bucketLinks);
				/* If we sent messages, it means we are also going to receive messages,
				 * then we will need to check for them, so hasMessages=true */
				hasMessages = true;
				numOfMsgs = bucketLinks.size();
				numOfWorkers = (int)(Math.ceil(numOfMsgs/100));
				/* I create a worker node for every 100 messages in the queue
				 * Computer type: Micro, Image: ami-76f0061f */
				instanceIDs = createWorkerNodes(numOfWorkers);
				/* This means we now have worker nodes, so we will need to check if they need to be deleted
				 * after they finished their job */
				hasNodes = true;
			}
			if(hasNodes)
				/* if there are workers still alive I delete them if the link queue is empty
				 * because that means that they have processed all of the messages I have gave them */
				checkSQSAndDeleteNodes(instanceIDs);
			/*TODO: weak point, do I wait till all messages are processed by the apps to obtain a result,
			 *      or do I meanwhile add more jobs from new applications. If I carry bringing more jobs
			 *      all the time, there can be a case that the messages being processes queue will never be
			 *      empty, so we will never obtain a result. */
			if(!hasMessages) {
				/* receive message from the SQS QueueThumbnails of PDF link, Thumbnail link and UUID of certain application
				 * Message Format from Worker: <pdf_link,s3_link,app_key(uuid of app)> 
				 * For each messages returned we update the array of appNums objects with incrementing by 1 the done jobs for that
				 * application, and adding the PDF link and Thumbnail link to its StringPair */
				receiveFromSQS2();
				/* For each application in the appNums array that its done jobs is equal to the number of PDFS it had to process
				 * we do the following. */
				for(AppNums app : appNums) {
					if(app.getNumberOfPDFs() == app.getCurrentNum()) {
						String uniqueName = UUID.randomUUID().toString();
						/* create output file of all pdfs and thumbnails of certain user
						 * The filename is a unique name (UUID) so there wont be files overwriting each other
						 * The output file format is: For Each Line: <pdf-link,s3-thumbnail-link> */
						File file = createOutputFile(app.links,uniqueName);
						/* upload file to S3 with the key being the uniqueName */
						uploadFileToS3(file,uniqueName);
						/* send message to the user queue with the place of the file
						 * Message Format to User: <AppKey=Application_UUID,Bucket=Bucket,Key=uniqueName> */
						createAndSendToSQS2(app.getKey(),uniqueName);
					}
				}
			}
		}
	}
}

package application;

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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
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
	static AmazonEC2      ec2;
	static AmazonS3		  s3;
	static AmazonSQS	  sqs;

	private static final String Bucket = Consts.LINKS_BUCKET_NAME;
	private static final String Key = UUID.randomUUID().toString();
	private static final String QueueOut = Consts.OUTPUT_QUEUE_NAME;
	private static final String QueueIn = Consts.INPUT_QUEUE_NAME;


	private static void init() throws Exception {
		AWSCredentials credentials = new PropertiesCredentials(
				Main.class.getResourceAsStream("/AwsCredentials.properties"));

		ec2 = new AmazonEC2Client(credentials);
		s3  = new AmazonS3Client(credentials);
		sqs = new AmazonSQSClient(credentials);

	}

	public static void uploadFileToS3(File file) throws IOException {
		try {
			CreateBucketRequest cbr = new CreateBucketRequest(Bucket);
			cbr.setCannedAcl(CannedAccessControlList.PublicRead);
			s3.createBucket(cbr);
			System.out.println("Uploading a new object to S3 from a file\n");
			s3.putObject(new PutObjectRequest(Bucket, Key, file));
		}
		catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:        " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:           " + ase.getErrorType());
			System.out.println("Request ID:           " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	public static ArrayList<ThumbPDF> downloadFromS3(String[] bucketInfo) throws IOException {
		List<ThumbPDF> outInfo = new ArrayList<ThumbPDF>();
		try {
			System.out.println("Download an object from S3\n");
			S3Object object = s3.getObject(new GetObjectRequest(bucketInfo[0], bucketInfo[1]));
			InputStream input = object.getObjectContent();
			BufferedReader reader = new BufferedReader(new InputStreamReader(input));
			while (true) {
				String line = reader.readLine();
				if (line == null) break;
				String[] temp = line.split(",");
				outInfo.add(new ThumbPDF(temp[0],temp[1]));
			}


		}
		catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:        " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:           " + ase.getErrorType());
			System.out.println("Request ID:           " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		return (ArrayList<ThumbPDF>) outInfo;
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
	
	public static String getManagerUserData() throws IOException
	{
		AWSCredentials credentials = new PropertiesCredentials(
				Main.class.getResourceAsStream("/AwsCredentials.properties"));
		
		String accessKey = credentials.getAWSAccessKeyId();
		String secretKey = credentials.getAWSSecretKey();
		
		InputStream managerUserDataInputStream = 
			Main.class.getResourceAsStream("/manager/start_manager.sh");
		BufferedReader br = new BufferedReader(new InputStreamReader(managerUserDataInputStream));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = br.readLine()) != null) {
			sb.append(line);
			sb.append('\n');
		}
				
		return Base64.encode(replace(replace(sb.toString(), "REPLACED_WITH_ACCESS_KEY", accessKey),
				"REPLACED_WITH_SECRET_KEY", secretKey));
	}

	public static void checkManagerInstance() throws IOException {
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		List<Tag> managerTag = new ArrayList<Tag>();
		List<String> valuesT1 = new ArrayList<String>();
		valuesT1.add("Manager1");
		Filter filter1 = new Filter("tag:Manager1", valuesT1);
		managerTag.add(new Tag("Manager1","Manager1"));

		DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter1));
		List<Reservation> reservations = result.getReservations();

		List<String> instanceID = new ArrayList<String>();

		if(reservations.isEmpty()) {//no manager node
			try {
				// ubuntu-maverick-10.10-amd64-server-20101225 (ami-cef405a7)
				RunInstancesRequest request2 = new RunInstancesRequest("ami-cef405a7", 1, 1);
				request2.setInstanceType(InstanceType.T1Micro.toString());			
				request2.setUserData(getManagerUserData());
			
				List<Instance> instances = ec2.runInstances(request2).getReservation().getInstances();
				for(Instance instance : instances) {
					System.out.println("Created manager instance " + instance.getInstanceId());
					instanceID.add(instance.getInstanceId());
				}
				CreateTagsRequest CTR = new CreateTagsRequest(instanceID,managerTag);
				ec2.createTags(CTR);
				System.out.println("Launch instances: " + instances);



			} catch (AmazonServiceException ase) {
				System.out.println("Caught Exception: " + ase.getMessage());
				System.out.println("Reponse Status Code: " + ase.getStatusCode());
				System.out.println("Error Code: " + ase.getErrorCode());
				System.out.println("Request ID: " + ase.getRequestId());
			}
		}
		else {
			System.out.println("instances!");
			for (Reservation reservation : reservations) {
				List<Instance> instances = reservation.getInstances();
				for (Instance instance : instances) {
					instance.getInstanceType();
				}
			}
		}

	}

	public static String createAndSendToSQS() throws IOException {
		try {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(QueueOut);
			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			String outMsg = "Bucket=" + Bucket + ",Key=" + Key;
			SendMessageRequest msg = new SendMessageRequest(myQueueUrl, outMsg);
			sqs.sendMessage(msg);
			return myQueueUrl;
		}
		catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
			"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:        " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:           " + ase.getErrorType());
			System.out.println("Request ID:           " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
			"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		return null;
	}


	public static String[] receiveFromSQS() throws IOException, Exception {
		String msg = null;
		String[] parsedMsg;
		String[] LparsedMsg;
		String[] RparsedMsg;
		String[] bucketInfo = new String[2];
		Exception badmsg = new Exception("bad message syntax");
		try {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(QueueIn);
			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
			for(;;) {
				List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				if(messages.size() == 0) {//queue is empty
					System.out.println("Queue is empty");
					Thread.sleep(1000);
				}
				else {
					for (Message message : messages) {
						if(message.getBody().contains(Key)) {
							msg = message.getBody();
							String messageRecieptHandle = message.getReceiptHandle();
							sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));
							break;
						}
					}
					if(msg != null)
						break;
					else {
						System.out.println("Queue doesnt have message you want");
						Thread.sleep(1000);
					}
				}
			}
			//System.out.println(msg);
			parsedMsg = msg.split(",");
			if(parsedMsg.length == 3) {
				LparsedMsg = parsedMsg[1].split("=");
				RparsedMsg = parsedMsg[2].split("=");
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

		}
		catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
			"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:        " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:           " + ase.getErrorType());
			System.out.println("Request ID:           " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
			"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return bucketInfo;
	}


	public static void createHTMLFile(String outPrefix, List<ThumbPDF> outInfo,int pageNumber,
			int itemsNumber, int numOfPages) throws IOException {
		String filename = outPrefix + pageNumber;
		try   {
			// Create file 
			FileWriter fstream = new FileWriter(filename + ".html");
			BufferedWriter out = new BufferedWriter(fstream);
			out.write("<html><head><title>Yarden and Koby's Lovely HTML File</title></head>" +
					"<body bgcolor=\"red\"><h1>" +
					"Page " + pageNumber +
					" (" + ((pageNumber-1)*(itemsNumber)) + " - " + ((pageNumber*itemsNumber)-1) +
			"</h1></br>");
			for(int i=1;i<=numOfPages;i++) {
				out.write("<a href=\"outPrefix" + i + ".html\">Page " + i + "</a>&nbsp;&nbsp;");
			}
			out.write("</br></br>");
			for(int i=0;i<itemsNumber;i++) {
				ThumbPDF tempTP = outInfo.get(i);
				out.write("<a href=\">" + tempTP.getPDF() + "\"><img src=\"" +
						tempTP.getThumbnail() + "\"></img></a>");
			}
			out.write("</body></html>");
			//Close the output stream
			out.close();
		}catch (Exception e){//Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}
	}

	public static void main(String[] args) throws Exception{
		if(args.length < 3) {
			System.out.println("enter <input_file> <output_prefix> <numPerFile>");
		}
		else {			
			String[] bucketInfo;
			File pdflinks = new File(args[0]);
			String numPerFile = args[2];
			int numPF = Integer.parseInt(numPerFile);
			int numOfPages;

			init();
			checkManagerInstance();
			uploadFileToS3(pdflinks);
			createAndSendToSQS();
			bucketInfo = receiveFromSQS();
			List<ThumbPDF> outInfo = downloadFromS3(bucketInfo);
			numOfPages = (int)(Math.ceil(outInfo.size()/numPF));

			for(int i=1;i<=numOfPages;i++) {
				List<ThumbPDF> tempList = outInfo.subList(((numPF-1)*i), (numPF*i)-1);				
				createHTMLFile(args[1],tempList,i,numPF,numOfPages);
			}

		}

	}

}

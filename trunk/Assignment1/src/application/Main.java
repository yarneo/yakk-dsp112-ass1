package application;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;


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
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class Main {
	static AmazonEC2      ec2;
	static AmazonS3		  s3;
	static AmazonSQS	  sqs;
	
	private static final String Bucket = "PDFs";
	private static final String Key = "key";
	private static final String QueueOut = "queueOut";
	private static final String QueueIn = "queueIn";


	private static void init() throws Exception {
		AWSCredentials credentials = new PropertiesCredentials(
				Main.class.getResourceAsStream("AwsCredentials.properties"));

		ec2 = new AmazonEC2Client(credentials);
		s3  = new AmazonS3Client(credentials);
		sqs = new AmazonSQSClient(credentials);

	}

	public static void uploadFileToS3(File file) throws IOException {
		try {
			s3.createBucket(Bucket);
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
	
	public static void downloadFromS3(String[] bucketInfo) throws IOException {
		try {
			s3.createBucket(bucketInfo[0]);
			System.out.println("Download an object from S3\n");
			//s3.getObject(getObjectRequest)
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
				// Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
				RunInstancesRequest request2 = new RunInstancesRequest("ami-76f0061f", 1, 1);
				request2.setInstanceType(InstanceType.T1Micro.toString());
				List<Instance> instances = ec2.runInstances(request2).getReservation().getInstances();
				for(Instance instance : instances) {
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
			sqs.sendMessage(new SendMessageRequest(myQueueUrl, outMsg));
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
	
	public static String[] recieveFromSQS() throws IOException, Exception {
		String msg;
		String[] parsedMsg;
		String[] LparsedMsg;
		String[] RparsedMsg;
		String[] bucketInfo = new String[2];
		Exception badmsg = new Exception("bad message syntax");
		try {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(QueueIn);
			String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            for(;;) {
            if(messages.size() == 0) {//queue is empty
                System.out.println("Queue is empty");
                Thread.sleep(1000);
            }
            else {
            	msg = messages.get(0).getBody();
            	break;

            }
            }
            parsedMsg = msg.split(",");
            if(parsedMsg.length == 2) {
            LparsedMsg = parsedMsg[0].split("=");
            RparsedMsg = parsedMsg[1].split("=");
            if(LparsedMsg[0] == "Bucket") 
            	bucketInfo[0] = parsedMsg[1];
            else {
            	throw badmsg;
            }
            if(RparsedMsg[1] == "Key") 
            	bucketInfo[1] = parsedMsg[1];
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


	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		//if(args.length < 2) {
		//	System.out.println("enter the input file");
		//}
		//else {
			String[] bucketInfo;
			//File pdflinks = new File(args[1]);
			init();
			//checkManagerInstance();
			//uploadFileToS3(pdflinks);
			createAndSendToSQS();
			bucketInfo = recieveFromSQS();
			downloadFromS3(bucketInfo);

		//}

	}

}

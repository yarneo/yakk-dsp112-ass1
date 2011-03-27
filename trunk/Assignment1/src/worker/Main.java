package worker;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.ImageIO;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import common.Consts;
import common.PDFTaskRequest;
import common.PDFTaskResponse;

public class Main {
	private static Logger logger = Logger.getLogger("worker.Main");
	/** The interval, in milliseconds, between queue polling attempts by the Worker. */
	private static final long WORKER_QUEUE_POLLING_INTERVAL = 5000;
	/** The prefix used for temporary file names. */
	private static final String WORKER_TEMP_FILE_NAME_PREFIX = "wrk";

	/**
	 * @param args
	 */
	public static void main(String[] args) {		
		/*
		 * get a PDF message from an SQS queue.
		 * download the PDF file indicated in the message.
		 * convert the first page of the PDF file into an image file.
		 * upload the image file to S3.
		 * put a message in an SQS queue indicating the original URL of the PDF and the S3 url of the new image file
		 * remove the PDF message from the SQS queue.
		 */	
		
		try {
			AWSCredentials creds = new PropertiesCredentials(
					Main.class.getResourceAsStream("/AwsCredentials.properties"));
			
			AmazonSQS sqs = new AmazonSQSClient(creds);
			AmazonS3 s3 = new AmazonS3Client(creds);
			
			if (!s3.doesBucketExist(Consts.WORKER_THUMBNAIL_BUCKET_NAME)) {
				CreateBucketRequest cbr = new CreateBucketRequest(Consts.WORKER_THUMBNAIL_BUCKET_NAME);
				cbr.setCannedAcl(CannedAccessControlList.PublicRead);
				s3.createBucket(cbr);				
			}			
			
			CreateQueueRequest createQueueRequest = 
				new CreateQueueRequest(Consts.WORKER_PDF_REQUEST_QUEUE_NAME);
			
			String requestQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			
			createQueueRequest = new CreateQueueRequest(Consts.WORKER_PDF_RESPONSE_QUEUE_NAME);
			String responseQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			
			while(true) {
				ReceiveMessageRequest rmr = new ReceiveMessageRequest(requestQueueUrl);
				
				List<Message> messages = sqs.receiveMessage(rmr).getMessages();
				if (messages.size() == 0) {
					Thread.sleep(WORKER_QUEUE_POLLING_INTERVAL);
				} else {
					for (Message msg : messages) {
						try {
							PDFTaskRequest taskRequest = new PDFTaskRequest(msg);
							
							PDDocument pdf = PDDocument.load(taskRequest.getPDFURL());
							@SuppressWarnings("unchecked")
							List<PDPage> allPages = 
								(List<PDPage>)pdf.getDocumentCatalog().getAllPages();
							
							PDPage firstPage;
							if (allPages.size() < 1) {
								firstPage = new PDPage();								
							} else {
								firstPage = allPages.get(0);
							}
												
							BufferedImage bimage = 
								firstPage.convertToImage(BufferedImage.TYPE_INT_RGB, 36);
							File imageFile = 
								File.createTempFile(WORKER_TEMP_FILE_NAME_PREFIX, null);
							
							ImageIO.write(bimage, "png", imageFile);
							
							String imageKey = UUID.randomUUID().toString() + ".png";
							
							s3.putObject(
									Consts.WORKER_THUMBNAIL_BUCKET_NAME,
									imageKey,
									imageFile);
							
							imageFile.delete();
							
							PDFTaskResponse response = new PDFTaskResponse(
									taskRequest.getPDFURL(),
									new URL(
											"http://" + 
											Consts.WORKER_THUMBNAIL_BUCKET_NAME + 
											".s3.amazonaws.com/" +
											imageKey),
									taskRequest.getUUID());
							
							SendMessageRequest smr = new SendMessageRequest(
									responseQueueUrl,
									response.toString());
							sqs.sendMessage(smr);

							DeleteMessageRequest dmr = new DeleteMessageRequest(requestQueueUrl, msg.getReceiptHandle());
							sqs.deleteMessage(dmr);			
						} catch (Exception e) {
							logger.log(Level.WARNING, "Error processing PDF task.", e);
						}
					}
				}
			}
		} catch (IOException ioe) {
			logger.log(Level.SEVERE, "I/O error in Worker.", ioe);			
			System.exit(1);
		} catch (InterruptedException ir) {
			logger.log(Level.SEVERE, "Worker interrupted.", ir);
			System.exit(2);
		} catch (AmazonServiceException ase) {
			logger.log(Level.SEVERE, "Amazon service error", ase);
			System.exit(3);
		} catch (AmazonClientException ace) {
			logger.log(Level.SEVERE, "Amazon client error", ace);
			System.exit(4);
		}
	}
}

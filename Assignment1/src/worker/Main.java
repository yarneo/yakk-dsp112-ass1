package worker;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.ImageIO;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.sun.pdfview.PDFFile;
import com.sun.pdfview.PDFPage;

import common.Consts;
import common.PDFTaskRequest;
import common.PDFTaskResponse;

public class Main {
	private static Logger logger = Logger.getLogger("worker.Main");
	/** The interval, in milliseconds, between queue polling attempts by the Worker. */
	private static final long WORKER_QUEUE_POLLING_INTERVAL = 30 * 1000; // 30 seconds
	/** The prefix used for temporary file names. */
	private static final String WORKER_TEMP_FILE_NAME_PREFIX = "wrk";
	/** The visibility timeout for worker messages, in seconds. */
	private static final int WORKER_VISIBILITY_TIMEOUT = 180;

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
				ReceiveMessageRequest rmr = 
					new ReceiveMessageRequest(requestQueueUrl)
						.withVisibilityTimeout(WORKER_VISIBILITY_TIMEOUT);
				
				List<Message> messages = sqs.receiveMessage(rmr).getMessages();
				if (messages.size() == 0) {
					Thread.sleep(WORKER_QUEUE_POLLING_INTERVAL);
				} else {
					logger.log(Level.INFO, "Worker received " + messages.size() + " messages.");
					for (Message msg : messages) {
						try {							
							logger.log(Level.INFO, "Worker received message: " +  msg.getBody());
							PDFTaskRequest taskRequest = new PDFTaskRequest(msg);

							URL url1 = taskRequest.getPDFURL();

							byte[] ba1 = new byte[1024];
							int baLength;
							FileOutputStream fos1 = new FileOutputStream("download.pdf");

							try {
								// Contacting the URL
								System.out.print("Connecting to " + url1.toString() + " ... ");
								URLConnection urlConn = url1.openConnection();

								// Checking whether the URL contains a PDF
								if (!urlConn.getContentType().equalsIgnoreCase("application/pdf")) {
									System.out.println("FAILED.\n[Sorry. This is not a PDF.]");
								} else {

										// Read the PDF from the URL and save to a local file
										InputStream is1 = url1.openStream();
										while ((baLength = is1.read(ba1)) != -1) {
											fos1.write(ba1, 0, baLength);
										}
										fos1.flush();
										fos1.close();
										is1.close();
								}
									} catch (Exception e) {
										System.out.println("FAILED.\n[" + e.getMessage() + "]");
									}

									File file = new File("download.pdf");
									File imageFile = 
										File.createTempFile(WORKER_TEMP_FILE_NAME_PREFIX, null);

									RandomAccessFile raf;
									try {
										raf = new RandomAccessFile(file, "r");

										FileChannel channel = raf.getChannel();
										ByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
										PDFFile pdffile = new PDFFile(buf);
										// draw the first page to an image
										PDFPage page = pdffile.getPage(0);

										//get the width and height for the doc at the default zoom				
										int width=(int)page.getBBox().getWidth();
										int height=(int)page.getBBox().getHeight();				

										Rectangle rect = new Rectangle(0,0,width,height);
										int rotation=page.getRotation();
										Rectangle rect1=rect;
										if(rotation==90 || rotation==270)
											rect1=new Rectangle(0,0,rect.height,rect.width);

										//generate the image
										BufferedImage img = (BufferedImage)page.getImage(
												rect.width, rect.height, //width & height
												rect1, // clip rect
												null, // null for the ImageObserver
												true, // fill background with white
												true  // block until drawing is done
										);
										
										ImageIO.write(img, "png", imageFile);
									}

									catch (FileNotFoundException e1) {
										System.err.println(e1.getLocalizedMessage());
									} catch (IOException e) {
										System.err.println(e.getLocalizedMessage());
									}
							
							
							
							
							
							String imageKey = "thumbnail-" + UUID.randomUUID().toString() + ".png";
							
							logger.log(Level.INFO, "Uploading " + imageKey + " to S3");
														
							PutObjectRequest por = new PutObjectRequest(
									Consts.WORKER_THUMBNAIL_BUCKET_NAME,
									imageKey,
									imageFile).withCannedAcl(CannedAccessControlList.PublicRead);
							s3.putObject(por);					
							
							imageFile.delete();
							
							PDFTaskResponse response = new PDFTaskResponse(
									taskRequest.getPDFURL(),
									new URL(
											"http://s3.amazonaws.com/" +
											Consts.WORKER_THUMBNAIL_BUCKET_NAME + "/" +
											imageKey),
									taskRequest.getUUID());
							
							SendMessageRequest smr = new SendMessageRequest(
									responseQueueUrl,
									response.toString());
							sqs.sendMessage(smr);
							
							logger.log(Level.INFO, "Worker done with " + taskRequest.getPDFURL());

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

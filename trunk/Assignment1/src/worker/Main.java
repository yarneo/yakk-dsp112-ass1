package worker;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
	/** The size of the worker's file buffer. */
	private static final int WORKER_FILE_BUFFER_SIZE = 4096;
	/** The timeout for downloading and generating a thumbnail for a single PDF. */
	private static final int WORKER_PDF_TIMEOUT = 10;
	/** The unit of timeout for downloading and thumbnailing a single PDF. */
	private static final TimeUnit WORKER_PDF_TIMEOUT_UNIT = TimeUnit.MINUTES;
	/** The name of the error thumbnail resource. */
	private static final String WORKER_ERROR_THUMBNAIL_NAME = "/ErrorThumbnail.png";
	/** The height of thumbnails. */
	private static final int WORKER_THUMBNAIL_HEIGHT = 200;
	/** The width of thumbnails. */
	private static final int WORKER_THUMBNAIL_WIDTH = 150;
	/** The maximum number of messages a worker reads from the queue each round. */
	private static final int WORKER_MAX_MESSAGES = 5;
	/** Amazon Simple Queue Service. */
	private static AmazonSQS sqs;
	/** Amazon Simple Storage Service. */
	private static AmazonS3 s3;
	
	public static String uploadPNG(BufferedImage thumbnail) throws IOException
	{
		File imageFile = 
			File.createTempFile(WORKER_TEMP_FILE_NAME_PREFIX, null);

		ImageIO.write(thumbnail, "png", imageFile);

		String imageKey = "thumbnail-" + UUID.randomUUID().toString() + ".png";

		logger.log(Level.INFO, "Uploading " + imageKey + " to S3");

		PutObjectRequest por = new PutObjectRequest(
				Consts.WORKER_THUMBNAIL_BUCKET_NAME,
				imageKey,
				imageFile).withCannedAcl(CannedAccessControlList.PublicRead);
		s3.putObject(por);					
		
		if (!imageFile.delete()) {
			logger.log(
					Level.WARNING,
					"Error deleting image file " + imageFile.getAbsolutePath());
		}
		
		return imageKey;
	}
	
	private static BufferedImage getPDFPageThumbnail(PDFPage page)
	{
		int pageWidth = (int)page.getBBox().getWidth();
		int pageHeight = (int)page.getBBox().getHeight();
		
		Rectangle clippingRectangle;
		int rotation = page.getRotation();
		
		if (rotation == 90 || rotation == 270) {
			clippingRectangle = new Rectangle(0, 0, pageHeight, pageWidth);
		} else {
			clippingRectangle = new Rectangle(0, 0, pageWidth, pageHeight);
		}
		
		int thumbnailWidth = WORKER_THUMBNAIL_WIDTH;
		int thumbnailHeight = WORKER_THUMBNAIL_HEIGHT;
		
		BufferedImage thumbnail = (BufferedImage)page.getImage(
				thumbnailWidth,
				thumbnailHeight,
				clippingRectangle,
				null,
				true,
				true 
				);
		
		return thumbnail;
	}
	
	private static ByteBuffer getPDFData(URL url) throws IOException
	{
		logger.log(Level.INFO, "Retrieving PDF from URL " + url.toString());	
		
		InputStream pdf = url.openStream();
		File pdfFile = 
			File.createTempFile(WORKER_TEMP_FILE_NAME_PREFIX, null);
		logger.log(
				Level.INFO, 
				"Writing PDF to temporary file " + pdfFile.getAbsolutePath());
		
		OutputStream os = new FileOutputStream(pdfFile);
		
		byte[] fileBuffer = new byte[WORKER_FILE_BUFFER_SIZE];
		int bytesRead;
		try {
			while ((bytesRead = pdf.read(fileBuffer)) != -1) {
				os.write(fileBuffer, 0, bytesRead);
			}
			os.flush();
			os.close();
			pdf.close();
		} catch (IOException ioe) {
			logger.log(Level.WARNING, "Error retrieving PDF " + url.toString(), ioe);
			throw ioe;
		}
		
		RandomAccessFile raf = new RandomAccessFile(pdfFile, "r");
		FileChannel fc = raf.getChannel();
		ByteBuffer buffer = fc.map(MapMode.READ_ONLY, 0, fc.size());
		
		if (!pdfFile.delete()) {
			pdfFile.deleteOnExit();
		}
		
		return buffer;
	}

	/**
	 * Main function of the Worker.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {		
		/*
		 * get a PDF message from an SQS queue.
		 * download the PDF file indicated in the message.
		 * convert the first page of the PDF file into an image file.
		 * upload the image file to S3.
		 * put a message in an SQS queue indicating the original URL of the PDF and
		 * the S3 url of the new image file
		 * remove the PDF message from the SQS queue.
		 */	
		
		try {
			AWSCredentials creds = new PropertiesCredentials(
					Main.class.getResourceAsStream("/AwsCredentials.properties"));
			
			sqs = new AmazonSQSClient(creds);
			s3 = new AmazonS3Client(creds);
			
			ExecutorService execService = Executors.newCachedThreadPool();
			
			if (!s3.doesBucketExist(Consts.WORKER_THUMBNAIL_BUCKET_NAME)) {
				CreateBucketRequest cbr = 
					new CreateBucketRequest(Consts.WORKER_THUMBNAIL_BUCKET_NAME);
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
						.withVisibilityTimeout(WORKER_VISIBILITY_TIMEOUT)
						.withMaxNumberOfMessages(WORKER_MAX_MESSAGES);
				
				List<Message> messages = sqs.receiveMessage(rmr).getMessages();
				if (messages.isEmpty()) {
					Thread.sleep(WORKER_QUEUE_POLLING_INTERVAL);
				} else {
					logger.log(Level.INFO, "Worker received " + messages.size() + " messages.");
					for (Message msg : messages) {
						try {							
							logger.log(Level.INFO, "Worker received message: " +  msg.getBody());
							final PDFTaskRequest taskRequest = new PDFTaskRequest(msg);
							
							Future<BufferedImage> future = execService.submit(
									new Callable<BufferedImage>() {
										public BufferedImage call() throws IOException
										{
											ByteBuffer pdfData = getPDFData(
													taskRequest.getPDFURL());
											
											PDFFile pdfFile = new PDFFile(pdfData);
											PDFPage firstPage = pdfFile.getPage(0);
											
											if (firstPage == null) {
												logger.log(
													Level.WARNING,
													"Could not retrieve first page of " +
													taskRequest.getPDFURL());
												return null;
											}
											
											BufferedImage img = getPDFPageThumbnail(firstPage);
											
											return img;
										}
									});
							
							BufferedImage img;
							try {
								img = future.get(WORKER_PDF_TIMEOUT, WORKER_PDF_TIMEOUT_UNIT);
							} catch (TimeoutException te) {
								if (!future.cancel(true)) {
									logger.log(
										Level.WARNING,
										"Error cancelling timed out PDF task for " + 
										taskRequest.getPDFURL());
								}
								img = null;
							} catch (Exception e) {
								logger.log(
									Level.WARNING,
									"PDF thumbnailing failed for " + taskRequest.getPDFURL(),
									e);
								
								img = null;
							}
							
							if (img == null) {
								img = ImageIO.read(
										Main.class.getResourceAsStream(
												WORKER_ERROR_THUMBNAIL_NAME));
							}
														
							String imageKey = uploadPNG(img);
							
							PDFTaskResponse response = new PDFTaskResponse(
									taskRequest.getPDFURL(),
									new URL(
											"http://s3.amazonaws.com/" +
											Consts.WORKER_THUMBNAIL_BUCKET_NAME + "/" +
											imageKey),
									taskRequest.getUUID());
							
							logger.log(
								Level.INFO,
								"Sending response message for " + taskRequest.getPDFURL());
							
							SendMessageRequest smr = new SendMessageRequest(
									responseQueueUrl,
									response.toString());
							sqs.sendMessage(smr);

							DeleteMessageRequest dmr = 
								new DeleteMessageRequest(requestQueueUrl, msg.getReceiptHandle());
							sqs.deleteMessage(dmr);
							
							logger.log(Level.INFO, "Worker done with " + taskRequest.getPDFURL());
						} catch (Exception e) {
							logger.log(
								Level.WARNING, "Error processing PDF task ", e);
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

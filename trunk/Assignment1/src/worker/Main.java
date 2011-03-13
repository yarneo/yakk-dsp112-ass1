package worker;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		/*
		 * get a PDF message from an SQS queue.
		 * download the PDF file indicated in the message.
		 * convert the first page of the PDF file into an image file.
		 * upload the image file to S3.
		 * put a message in an SQS queue indicating the original URL of the PDF and the S3 url of the new image file
		 * remove the PDF message from the SQS queue.
		 */
		

	}

}

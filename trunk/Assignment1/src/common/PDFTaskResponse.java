package common;

import java.net.URL;

public class PDFTaskResponse {
	private URL originalPDF;
	private URL convertedImage;
	private String uuid;
	
	
	public PDFTaskResponse(URL originalPDF, URL convertedImage, String uuid)
	{
		this.originalPDF = originalPDF;
		this.convertedImage = convertedImage;
		this.uuid = uuid;
	}
	
	public String toString()
	{
		return originalPDF + "," + convertedImage + "," + uuid;
	}
}

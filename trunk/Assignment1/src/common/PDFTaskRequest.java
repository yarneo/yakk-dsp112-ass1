package common;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.sqs.model.Message;

public class PDFTaskRequest {
	private static Pattern p = Pattern.compile("App=([^,]*),Link=(.*)");
	private String uuid;
	private URL url;
	
	public PDFTaskRequest(Message msg) throws MalformedURLException
	{				
		Matcher m = p.matcher(msg.getBody());
		
		if (m.matches()) {
			uuid = m.group(1);
			url = new URL(m.group(2));	
		} else {
			throw new IllegalStateException();
		}
	}
	
	public String getUUID()
	{
		return uuid;
	}
	
	public URL getPDFURL()
	{
		return url;
	}
}

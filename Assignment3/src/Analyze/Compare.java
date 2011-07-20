package Analyze;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;


public class Compare {

	public static final String inp1 = "http://www.textfiles.com/food/1st_aid.txt";
	public static final String inp2 = "http://www.textfiles.com/food/acetab1.txt";
	public static final String out = "out.txt";

	//	Word/tag pairs where p(t|w) is high in the uniform initalization but not in the more complex one.
	public static ArrayList<String> highuni = new ArrayList<String>();
	//	Word/tag pairs where p(t|w) is high in the complex initalization but not in the uniform one.
	public static ArrayList<String> highcomp = new ArrayList<String>();
	//	Word/tag pairs where p(t|w) is near zero in the complex initalization but not in the uniform one.
	public static ArrayList<String> lowcomp = new ArrayList<String>();
	//	Word/tag pairs where p(t|w) is near zero in the uniform initalization but not in the more complex one.
	public static ArrayList<String> lowuni = new ArrayList<String>();

	static protected BufferedReader initialize(String input) {
		URL url;
		InputStream is;
		InputStreamReader isr;
		BufferedReader r = null;
		try {
			System.out.println("Reading URL: " + input);
			url = new URL(input);
			is = url.openStream();
			isr = new InputStreamReader(is);
			r = new BufferedReader(isr);
		} catch (MalformedURLException e) {
			System.out.println("Invalid URL");
		} catch (IOException e) {
			System.out.println("Can not connect");
		}
		return r;
	}
	static public boolean allow(String word, String tag) {
		return MapReduce.Common.allow(tag, word) == 1;
	}

	static public boolean highCase(double num) {
		return (num > 0.9);
	}

	static public boolean lowCase(String word,String tag,double num) {
		return ((allow(word,tag)) && (num < 0.0001));
	}

	static public void main(String args[]) {
		BufferedReader b1 = initialize(inp1);
		BufferedReader b2 = initialize(inp2);
		String str1 = "";
		String str2 = "";
		try {
			do {
				str1 = b1.readLine();
				str2 = b2.readLine();
				if ((str1 != null) && (str2 != null)) {
					String[] strarr1 = str1.split("\\s+");
					String[] strarr2 = str2.split("\\s+");
					if((!strarr1[0].equals(strarr2[0])) || (strarr1.length != strarr2.length)) {
						System.out.println("Houston, We have a problem.");
						return;
					}
					for(int i=1;i<strarr1.length;i+=2) {
						String word = strarr1[0];
						String tmptag1 = strarr1[i];
						String tmptag2 = strarr2[i];
						double tmpnum1 = Double.parseDouble(strarr1[i+1]);
						double tmpnum2 = Double.parseDouble(strarr2[i+1]);
						boolean out1 = highCase(tmpnum1);
						boolean out2 = highCase(tmpnum2);
						boolean out3 = lowCase(word,tmptag1,tmpnum1);
						boolean out4 = lowCase(word,tmptag2,tmpnum2);
						if(out1 && !out2) {
							highuni.add(word + " " + tmptag1);
						}
						else if(!out1 && out2) {
							highcomp.add(word + " " + tmptag2);
						}
						if(out3 && !out4) {
							lowuni.add(word + " " + tmptag1);
						}
						else if(!out3 && out4) {
							lowcomp.add(word + " " + tmptag2);
						}			
					}				
				}
				else {
					System.out.println("Houston, We have a problem.");
					return;
				}
			} while ((str1 != null) && (str2 != null) );
		} catch (Exception e) {
			System.out.println("Can not connect");
		}
		
		
		  try{
			  // Create file 
			  FileWriter fstream = new FileWriter(out);
			  BufferedWriter out = new BufferedWriter(fstream);
			  out.write("p(t|w) is high in the uniform initalization but not in the more complex one\n");
			  for(String tmp : highuni) {
				  out.write(tmp + "\n");
			  }
			  out.write("p(t|w) is high in the complex initalization but not in the uniform one\n");
			  for(String tmp : highcomp) {
				  out.write(tmp + "\n");
			  }
			  out.write("p(t|w) is near zero in the complex initalization but not in the uniform one\n");
			  for(String tmp : lowcomp) {
				  out.write(tmp + "\n");
			  }
			  out.write("p(t|w) is near zero in the uniform initalization but not in the more complex one\n");
			  for(String tmp : lowuni) {
				  out.write(tmp + "\n");
			  }
			  //Close the output stream
			  out.close();
			  }catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
			  }
		
		
	}



}

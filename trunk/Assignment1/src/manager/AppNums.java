package manager;

import java.util.ArrayList;

public class AppNums {

	private String key;
	private int numberOfPDFs;
	private int currentNum;
	public ArrayList<BucketKey> links = new ArrayList<BucketKey>(); 
	
	
	public AppNums(String key, int numberOfPDFs) {
		this.key = key;
		this.numberOfPDFs = numberOfPDFs;
		this.currentNum = 0;
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	
	public int getNumberOfPDFs() {
		return numberOfPDFs;
	}
	public void setNumberOfPDFs(int numberOfPDFs) {
		this.numberOfPDFs = numberOfPDFs;
	}
	
	public int getCurrentNum() {
		return currentNum;
	}

	public void setCurrentNum(int currentNum) {
		this.currentNum = currentNum;
	}

}

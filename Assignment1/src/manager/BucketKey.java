package manager;

public class BucketKey {
	
	private String bucket;
	private String key;

	public BucketKey(String bucket, String key) {
		this.bucket = bucket;
		this.key = key;
	}

	public String getBucket() {
		return bucket;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

}

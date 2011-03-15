package application;

public class ThumbPDF {
	
	private String thumbnail;
	private String PDF;

	public ThumbPDF(String thumbnail, String PDF) {
		this.thumbnail = thumbnail;
		this.PDF = PDF;
	}

	public String getThumbnail() {
		return thumbnail;
	}

	public void setThumbnail(String thumbnail) {
		this.thumbnail = thumbnail;
	}

	public String getPDF() {
		return PDF;
	}

	public void setPDF(String pDF) {
		PDF = pDF;
	}

}

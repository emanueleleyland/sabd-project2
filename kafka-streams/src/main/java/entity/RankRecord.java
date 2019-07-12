package entity;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class RankRecord implements Serializable {

	private static final long serialVersionUID = 1L;

	public String articleID;
	public Long nComments;
	public String timestamp;

	public RankRecord(String articleID, Long nComments, Long timestamp) {
		this.articleID = articleID;
		this.nComments = nComments;
		LocalDateTime time = Instant.ofEpochMilli(timestamp).atZone(ZoneId.of("UTC")).toLocalDateTime();
		this.timestamp = time.toString();
	}

	public RankRecord() {}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getArticleID() {
		return articleID;
	}

	public void setArticleID(String articleID) {
		this.articleID = articleID;
	}

	public Long getnComments() {
		return nComments;
	}

	public void setnComments(Long nComments) {
		this.nComments = nComments;
	}

	@Override
	public String toString() {
		return "RankRecord{" +
				"articleID='" + articleID + '\'' +
				", nComments=" + nComments +
				", timestamp=" + timestamp +
				'}';
	}
}
package entity;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;


public class Comment implements Serializable {

	private static final long serialVersionUID = 1L;

	private String approveDate;
	private String articleID;
	private String articleWordCount;
	private String commentID;
	private String commentType;
	private String createDate;
	private String depth;
	private String editorsSelection;
	private String inReplyTo;
	private String parentUserDisplayName;
	private String recommendations;
	private String sectionName;
	private String userDisplayName;
	private String userID;
	private String userLocation;

	public static Comment parseJsonToObject(String json) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.readValue(json, Comment.class);
	}

	public boolean isValidComment() {
		Long res = null;
		Boolean bool = null;
		try {
			res = Long.valueOf(this.approveDate);
			res = Long.valueOf(this.articleWordCount);
			res = Long.valueOf(this.commentID);
			res = Long.valueOf(this.createDate);
			res = Long.valueOf(this.depth);
			bool = Boolean.valueOf(this.editorsSelection.toLowerCase());
			res = Long.valueOf(this.inReplyTo);
			res = Long.valueOf(this.recommendations);
			res = Long.valueOf(this.userID);
		} catch (NumberFormatException e){
			return false;
		}
		return true;
	}


	/*public Tuple<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String> toTuple15() throws NumberFormatException{
		return new Tuple15<>(
				Long.valueOf(this.approveDate),
				this.articleID,
				Long.valueOf(this.articleWordCount),
				Long.valueOf(this.commentID),
				this.commentType,
				Long.valueOf(this.createDate),
				Long.valueOf(this.depth),
				Boolean.valueOf(this.editorsSelection.toLowerCase()),
				Long.valueOf(this.inReplyTo),
				this.parentUserDisplayName,
				Long.valueOf(this.recommendations),
				this.sectionName,
				this.userDisplayName,
				Long.valueOf(this.userID),
				this.userLocation
		);
	}*/

	public String getApproveDate() {
		return approveDate;
	}

	public void setApproveDate(String approveDate) {
		this.approveDate = approveDate;
	}

	public String getArticleID() {
		return articleID;
	}

	public void setArticleID(String articleID) {
		this.articleID = articleID;
	}

	public String getArticleWordCount() {
		return articleWordCount;
	}

	public void setArticleWordCount(String articleWordCount) {
		this.articleWordCount = articleWordCount;
	}

	public String getCommentID() {
		return commentID;
	}

	public void setCommentID(String commentID) {
		this.commentID = commentID;
	}

	public String getCommentType() {
		return commentType;
	}

	public void setCommentType(String commentType) {
		this.commentType = commentType;
	}

	public String getCreateDate() {
		return createDate;
	}

	public void setCreateDate(String createDate) {
		this.createDate = createDate;
	}

	public String getDepth() {
		return depth;
	}

	public void setDepth(String depth) {
		this.depth = depth;
	}

	public String getEditorsSelection() {
		return editorsSelection;
	}

	public void setEditorsSelection(String editorsSelection) {
		this.editorsSelection = editorsSelection;
	}

	public String getInReplyTo() {
		return inReplyTo;
	}

	public void setInReplyTo(String inReplyTo) {
		this.inReplyTo = inReplyTo;
	}

	public String getParentUserDisplayName() {
		return parentUserDisplayName;
	}

	public void setParentUserDisplayName(String parentUserDisplayName) {
		this.parentUserDisplayName = parentUserDisplayName;
	}

	public String getRecommendations() {
		return recommendations;
	}

	public void setRecommendations(String recommendations) {
		this.recommendations = recommendations;
	}

	public String getSectionName() {
		return sectionName;
	}

	public void setSectionName(String sectionName) {
		this.sectionName = sectionName;
	}

	public String getUserDisplayName() {
		return userDisplayName;
	}

	public void setUserDisplayName(String userDisplayName) {
		this.userDisplayName = userDisplayName;
	}

	public String getUserID() {
		return userID;
	}

	public void setUserID(String userID) {
		this.userID = userID;
	}

	public String getUserLocation() {
		return userLocation;
	}

	public void setUserLocation(String userLocation) {
		this.userLocation = userLocation;
	}

	public Comment() {
	}

	@Override
	public String toString() {
		return "Comment{" +
				"approveDate='" + approveDate + '\'' +
				", articleID='" + articleID + '\'' +
				", articleWordCount='" + articleWordCount + '\'' +
				", commentID='" + commentID + '\'' +
				", commentType='" + commentType + '\'' +
				", createDate='" + createDate + '\'' +
				", depth='" + depth + '\'' +
				", editorsSelection='" + editorsSelection + '\'' +
				", inReplyTo='" + inReplyTo + '\'' +
				", parentUserDisplayName='" + parentUserDisplayName + '\'' +
				", recommendations='" + recommendations + '\'' +
				", sectionName='" + sectionName + '\'' +
				", userDisplayName='" + userDisplayName + '\'' +
				", userID='" + userID + '\'' +
				", userLocation='" + userLocation + '\'' +
				'}';
	}
}

package Project2.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple15;

import java.io.IOException;

/**
 * The single comment (or entry) of the dataset
 */
public class Comment {

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

    /**
     * Method to parse a json-formatted object to a Comment class
     * @param json: the json string to parse
     * @return: the comment object
     */
    public static Comment parseJsonToObject(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, Comment.class);
    }


    /**
     * @return the object in a Tuple15 flink format
     */
    public Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String> toTuple15() throws NumberFormatException{
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
    }

    public Comment(String approveDate, String articleID, String articleWordCount, String commentID, String commentType, String createDate, String depth, String editorsSelection, String inReplyTo, String parentUserDisplayName, String recommendations, String sectionName, String userDisplayName, String userID, String userLocation) {
        this.approveDate = approveDate;
        this.articleID = articleID;
        this.articleWordCount = articleWordCount;
        this.commentID = commentID;
        this.commentType = commentType;
        this.createDate = createDate;
        this.depth = depth;
        this.editorsSelection = editorsSelection;
        this.inReplyTo = inReplyTo;
        this.parentUserDisplayName = parentUserDisplayName;
        this.recommendations = recommendations;
        this.sectionName = sectionName;
        this.userDisplayName = userDisplayName;
        this.userID = userID;
        this.userLocation = userLocation;
    }

    /* getters and setters for the objects */

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
}

package entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.bean.CsvBindByName;
import lombok.*;

/**
 * The single comment record class in the dataset. This class is used as a Java Bean to read csv dataset entry
 * and send this one to kafka.
 */

@Getter
@Setter
@ToString
@NoArgsConstructor
public class Tuple {

    @CsvBindByName
    String approveDate;

    @CsvBindByName
    String articleID;

    @CsvBindByName
    String articleWordCount;

    @CsvBindByName
    String commentID;

    @CsvBindByName
    String commentType;

    @CsvBindByName
    String createDate;

    @CsvBindByName
    String depth;

    @CsvBindByName
    String editorsSelection;

    @CsvBindByName
    String inReplyTo;

    @CsvBindByName
    String parentUserDisplayName;

    @CsvBindByName
    String recommendations;

    @CsvBindByName
    String sectionName;

    @CsvBindByName
    String userDisplayName;

    @CsvBindByName
    String userID;

    @CsvBindByName
    String userLocation;

    public Tuple(String articleID, String createDate) {
        this.articleID = articleID;
        this.createDate = createDate;
    }

    public String toJson() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(this);
    }

    public Tuple(String approveDate, String articleID, String articleWordCount, String commentID, String commentType, String createDate, String depth, String editorsSelection, String inReplyTo, String parentUserDisplayName, String recommendations, String sectionName, String userDisplayName, String userID, String userLocation) {
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

    public Tuple() {
    }

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
}

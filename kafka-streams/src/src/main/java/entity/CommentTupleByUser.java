package entity;

import java.io.Serializable;

public class CommentTupleByUser implements Serializable {

	private static final long serialVersionUID = 1L;

	public Long inReplyTo;
	public Double indirectCount;
	public Double recommendations;
	public Long userId;

	public CommentTupleByUser(Long inReplyTo, Double indirectCount, Double recommendations, Long userId) {
		this.inReplyTo = inReplyTo;
		this.indirectCount = indirectCount;
		this.recommendations = recommendations;
		this.userId = userId;
	}

	public CommentTupleByUser() {
	}

	public static long getSerialVersionUID() {
		return serialVersionUID;
	}

	public Long getInReplyTo() {
		return inReplyTo;
	}

	public void setInReplyTo(Long inReplyTo) {
		this.inReplyTo = inReplyTo;
	}

	public Double getIndirectCount() {
		return indirectCount;
	}

	public void setIndirectCount(Double indirectCount) {
		this.indirectCount = indirectCount;
	}

	public Double getRecommendations() {
		return recommendations;
	}

	public void setRecommendations(Double recommendations) {
		this.recommendations = recommendations;
	}

	public Long getUserId() {
		return userId;
	}

	public void setUserId(Long userId) {
		this.userId = userId;
	}

	@Override
	public String toString() {
		return "CommentTupleByUser{" +
				"inReplyTo=" + inReplyTo +
				", indirectCount=" + indirectCount +
				", recommendations=" + recommendations +
				", userId=" + userId +
				'}';
	}
}

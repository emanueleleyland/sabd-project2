package entity;

import java.io.Serializable;

public class DirectCommentTuple implements Serializable {

	private static final long serialVersionUID = 1L;

	public Long userId;
	public Double recommendations;


	public DirectCommentTuple(Long userId, Double recommendations) {
		this.userId = userId;
		this.recommendations = recommendations;
	}

	public static long getSerialVersionUID() {
		return serialVersionUID;
	}

	public Long getUserId() {
		return userId;
	}

	public void setUserId(Long userId) {
		this.userId = userId;
	}

	public Double getRecommendations() {
		return recommendations;
	}

	public void setRecommendations(Double recommendations) {
		this.recommendations = recommendations;
	}

	@Override
	public String toString() {
		return "DirectCommentTuple{" +
				"userId=" + userId +
				", recommendations=" + recommendations +
				'}';
	}
}

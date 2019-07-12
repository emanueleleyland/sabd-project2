package entity;


import java.io.Serializable;

public class RedisBean implements Serializable {

	private static final long serialversionUID = 1L;

	Long userId;
	Double recommendations;
	Long inReplyTo;

	public RedisBean(Long userId, Double recommendations, Long inReplyTo) {
		this.userId = userId;
		this.recommendations = recommendations;
		this.inReplyTo = inReplyTo;
	}

	public RedisBean() {
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

	public Long getInReplyto() {
		return inReplyTo;
	}

	public void setInReplyto(Long inReplyto) {
		this.inReplyTo = inReplyto;
	}
}

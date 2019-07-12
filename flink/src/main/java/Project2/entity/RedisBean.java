package Project2.entity;

/**
 * Class used to serialize the single entry in redis remote DB in the
 * third query.
 */
public class RedisBean {

	//the ID of the user that produced the comment
	Long userId;

	//number of likes for the given comment.
	// The value is 0 if the comment is indirect
	Double recommendations;

	//ID of the user that received the response
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

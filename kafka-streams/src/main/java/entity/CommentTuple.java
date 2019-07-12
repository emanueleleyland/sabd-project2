package entity;

import java.io.Serializable;

public class CommentTuple implements Serializable {

	private static final long serialVersionUID = 1L;

	public Long userId;
	public Double recommendations;
	public Double count;

	public CommentTuple(Long userId, Double recommendations, Double count) {
		this.userId = userId;
		this.recommendations = recommendations;
		this.count = count;
	}

	@Override
	public String toString() {
		return "CommentTuple{" +
				"userId=" + userId +
				", recommendations=" + recommendations +
				", count=" + count +
				'}';
	}
}

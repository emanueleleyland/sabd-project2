package entity;

import java.io.Serializable;

public class CommentRank implements Serializable {

	private static final long serialVersionUID = 1L;

	public long userId;
	public double score;

	public CommentRank(Long userId, Double score) {
		this.userId = userId;
		this.score = score;
	}

	public CommentRank() {
	}

	public static long getSerialVersionUID() {
		return serialVersionUID;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}


	@Override
	public String toString() {
		return "CommentRank{" +
				"userId=" + userId +
				", score=" + score +
				'}';
	}
/*
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		CommentRank that = (CommentRank) o;
		return Objects.equals(userId, that.userId) &&
				Objects.equals(score, that.score);
	}

	@Override
	public int hashCode() {
		return Objects.hash(userId, score);
	}*/
}

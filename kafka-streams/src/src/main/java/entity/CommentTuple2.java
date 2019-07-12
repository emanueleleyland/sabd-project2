package entity;

import java.io.Serializable;

public class CommentTuple2 implements Serializable {

	private static final long serialVersionUID = 1L;

	public Long userIdOrCount;
	public Long inReplyTo;

	public CommentTuple2(Long userIdOrCount, Long inReplyTo) {
		this.userIdOrCount = userIdOrCount;
		this.inReplyTo = inReplyTo;
	}

	@Override
	public String toString() {
		return "CommentTuple2{" +
				"userIdOrCount=" + userIdOrCount +
				", inReplyTo=" + inReplyTo +
				'}';
	}
}

package entity;

import java.io.Serializable;

public class IndirectComments2 implements Serializable {

	private static final long serialVersionUID = 1L;

	public Long userId, inReplyTo;

	public IndirectComments2(Long userId, Long inReplyTo) {
		this.userId = userId;
		this.inReplyTo = inReplyTo;
	}

	@Override
	public String toString() {
		return "IndirectComments2{" +
				"userId=" + userId +
				", inReplyTo=" + inReplyTo +
				'}';
	}
}

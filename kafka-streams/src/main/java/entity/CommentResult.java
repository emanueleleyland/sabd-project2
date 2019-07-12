package entity;

import java.io.Serializable;

public class CommentResult implements Serializable {

	private static final long serialVersionUID = 1L;

	public Double indirectCount;
	public Double recommendations;

	public CommentResult(Double indirectCount, Double recommendations) {
		this.indirectCount = indirectCount;
		this.recommendations = recommendations;
	}
}

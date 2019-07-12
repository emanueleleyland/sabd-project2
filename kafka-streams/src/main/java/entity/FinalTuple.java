package entity;

import java.io.Serializable;

public class FinalTuple implements Serializable {

	private static final long serialVersionUID = 1L;

	public Double recommendations, count;

	public FinalTuple(Double recommendations, Double count) {
		this.recommendations = recommendations;
		this.count = count;
	}

	@Override
	public String toString() {
		return "FinalTuple{" +
				"recommendations=" + recommendations +
				", count=" + count +
				'}';
	}
}

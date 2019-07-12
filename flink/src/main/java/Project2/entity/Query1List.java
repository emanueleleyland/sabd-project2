package Project2.entity;

import java.io.Serializable;
import java.util.List;

/**
 * Bean class used to serialize the result of the all queries
 */
public class Query1List implements Serializable {


	Long time; //timestamp of the emitted result
	List<Query1Bean> list; //list of key-value results

	public Query1List(Long time, List<Query1Bean> list) {
		this.time = time;
		this.list = list;
	}

	public Query1List() {
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public List<Query1Bean> getList() {
		return list;
	}

	public void setList(List<Query1Bean> list) {
		this.list = list;
	}
}

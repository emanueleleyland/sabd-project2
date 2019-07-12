package Project2.entity;

import java.io.Serializable;

/**
 * Bean class used to serialize the result of a single element in the list of the query results
 * @param <T>: the type of the Key of the object
 * @param <V>: the type of the Value of the object
 */
public class Query1Bean<T, V> implements Serializable {
	T id;
	V count;

	public Query1Bean(T id, V count) {
		this.id = id;
		this.count = count;
	}

	public Query1Bean() {
	}

	public T getId() {
		return id;
	}

	public void setId(T id) {
		this.id = id;
	}

	public V getCount() {
		return count;
	}

	public void setCount(V count) {
		this.count = count;
	}
}

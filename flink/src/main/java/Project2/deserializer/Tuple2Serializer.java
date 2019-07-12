package Project2.deserializer;

import Project2.entity.Query1Bean;
import Project2.entity.Query1List;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Serializer class to write on kafka the results of the three query
 * @param <T> Type of the key of result list of the query
 * @param <V> Type of the value of result list of the query
 */
public class Tuple2Serializer<T, V> implements SerializationSchema<Tuple2<Long, List<Tuple2<T, V>>>> {
	@Override
	public byte[] serialize(Tuple2<Long, List<Tuple2<T, V>>> tuple) {
		ObjectMapper mapper =  new ObjectMapper();
		byte[] bytes =  new byte[0];
		List<Query1Bean> list = new ArrayList<>();
		tuple.f1.forEach(t -> list.add(new Query1Bean<T, V>(t.f0, t.f1)));
		Query1List query1List = new Query1List(tuple.f0, list);
		try {
			bytes = mapper.writeValueAsBytes(query1List);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return bytes;
	}

}

package Project2.deserializer;

import Project2.entity.Comment;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.io.IOException;

/**
 * Comment tuple deserializer from kafka.
 */
public class CommentDeserializer implements DeserializationSchema<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String>> {
    private static final long serialVersionUID = 6154188370181669758L;


    /**
     * Deserializer method
     * @param bytes: stream read by kafka consumer
     * @return the comment object in flink tuple format
     */
    @Override
    public Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String> deserialize(byte[] bytes) throws IOException {
        String json = new String(bytes);
        Comment comment = Comment.parseJsonToObject(json);
        Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String> res;
        try {
            res = comment.toTuple15();
        } catch (NumberFormatException e){
            res = null;
        }
        return res;
    }

    @Override
    public boolean isEndOfStream(Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String> longStringLongLongStringLongLongStringLongStringLongStringStringLongStringTuple15) {
        return false;
    }

    /**
     * Return the type information of Comment class
     */
    @Override
    public TypeInformation<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String>> getProducedType() {
        return new TupleTypeInfo<>(
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(Boolean.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class));
    }
}

package com.haima.sage.bigdata.etl.plugin.es_6.utils;

import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import scala.Array;
import scala.Tuple2;
import scala.collection.Iterable;
import scala.collection.Iterator;

import java.io.IOException;
import java.util.Map;

public class ScalaXContentBuilder {
    private XContentBuilder builder;

    public ScalaXContentBuilder() throws IOException {
        this(JsonXContent.jsonXContent);
    }

    private ScalaXContentBuilder(XContent xContent) throws IOException {
        if (xContent == null) {
            xContent = JsonXContent.jsonXContent;
        }
        builder = XContentBuilder.builder(xContent);
    }

    public XContentBuilder bytes(Map<String, Object> map) throws IOException {
      //TODO check  EnsureNoSelfReferences.ensureNoSelfReferences(map);
        dealMap(map);
        return builder;
    }


    private void dealMap(Map<String, Object> values) throws IOException {
        if (values != null) {
            builder.startObject();
            for (Map.Entry<String, Object> value : values.entrySet()) {
                builder.field(value.getKey());
                unknownValue(value.getValue());
            }
            builder.endObject();
        }else{
            builder.nullValue();
        }
    }


    private void unknownValue(Object value) throws IOException {
        if (value == null) {
            builder.nullValue();
        }else if (value instanceof scala.collection.Map) {
            dealScalaMap((scala.collection.Map) value);
        } else if (value instanceof scala.Array) {
            dealScalaArray((scala.Array) value);
        } else if (value instanceof scala.collection.Iterable) {
            dealScalaIterable((scala.collection.Iterable) value);
        } else {
            builder.value(value);
        }
    }

    private void dealScalaIterable(Iterable value) throws IOException {
        if (value != null) {
            builder.startArray();
            Iterator iterator = value.iterator();
            while (iterator.hasNext()) {
                Object tuple2 = iterator.next();
                unknownValue(tuple2);
            }
            builder.endArray();
        }else{
            builder.nullValue();
        }
    }

    private void dealScalaArray(Array value) throws IOException {
        builder.startArray();
        for (int i = 0; i < value.length(); i++) {
            Object item = value.apply(i);
            unknownValue(item);
        }
        builder.endArray();
    }

    private void dealScalaMap(scala.collection.Map map) throws IOException {
        if (map != null) {
            builder.startObject();
            Iterator<Tuple2> iterator = map.iterator();
            while (iterator.hasNext()) {
                Tuple2 tuple = iterator.next();

                builder.field(tuple._1().toString());
                unknownValue(tuple._2());
            }
            builder.endObject();
        }else{
            builder.nullValue();
        }
    }

}

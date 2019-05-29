package com.haima.sage.bigdata.etl.plugin.es_6.utils;

import java.nio.file.Path;
import java.util.*;

class EnsureNoSelfReferences {

    static void ensureNoSelfReferences(Object value) {
        Iterable<?> it = convert(value);
        if (it != null) {
            ensureNoSelfReferences(it, value, Collections.newSetFromMap(new IdentityHashMap<>()));
        }
    }

    private static Iterable<?> convert(Object value) {
        if (value == null) {
            return null;
        }else if (value instanceof Map) {
            return ((Map<?, ?>) value).values();
        } else if ((value instanceof Iterable) && (!(value instanceof Path))) {
            return (Iterable<?>) value;
        } else if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        } else {
            return null;
        }
    }

    private static void ensureNoSelfReferences(final Iterable<?> value, Object originalReference, final Set<Object> ancestors) {
        if (value != null) {
            if (!ancestors.add(originalReference)) {
                throw new IllegalArgumentException("Iterable object is self-referencing itself");
            }
            for (Object o : value) {
                ensureNoSelfReferences(convert(o), o, ancestors);
            }
            ancestors.remove(originalReference);
        }
    }
}

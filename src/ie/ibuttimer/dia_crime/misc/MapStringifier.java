/*
 * The MIT License (MIT)
 * Copyright (c) 2020 Ian Buttimer
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ie.ibuttimer.dia_crime.misc;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MapStringifier<K, V> {

    public static final String KVP_SEPARATOR = ", ";
    public static final String KV_SEPARATOR = ":";

    private Map<K, V> map;

    public MapStringifier(Map<K, V> map) {
        this.map = map;
    }

    public String stringify() {
        return MapStringifier.stringify(map);
    }

    public static <X, Y> String stringify(Map<X, Y> map) {
        return MapStringifier.stringBuildify(map).toString();
    }

    public static <X, Y> StringBuilder stringBuildify(Map<X, Y> map) {
        StringBuilder sb = new StringBuilder();
        map.forEach((column, value) -> {
            if (sb.length() > 0) {
                sb.append(KVP_SEPARATOR);
            }
            sb.append(column)
                    .append(KV_SEPARATOR)
                    .append(value);
        });
        return sb;
    }

    public static Map<String, String> mapify(String line, Map<String, String> map) {
        String[] keyValPairs = line.split(KVP_SEPARATOR);
        Arrays.stream(keyValPairs).forEach(kvp -> {
            String[] keyVal = kvp.trim().split(KV_SEPARATOR);
            if (keyVal.length == 2) {
                map.put(keyVal[0], keyVal[1]);
            }
        });

        return map;
    }

    public static Map<String, String> mapify(String line) {
        return mapify(line, new HashMap<>());
    }

    public static ElementStringify elementStringifier() {
        return new ElementStringify(KV_SEPARATOR);
    }


    public static class ElementStringify {

        private String separator;

        public ElementStringify(String separator) {
            this.separator = separator;
        }

        public static ElementStringify of(String separator) {
            return new ElementStringify(separator);
        }

        public String stringifyElement(String field, String value) {
            return field+separator+value;
        }

        public Pair<String, String> destringifyElement(String element) {
            Pair<String, String> result;
            String[] splits = element.split(separator);
            if (splits.length == 2) {
                result = Pair.of(splits[0], splits[1]);
            } else {
                result = Pair.of(null, null);
            }
            return result;
        }
    }
}

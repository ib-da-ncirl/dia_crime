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

/**
 * Utility class to manipulate key/value separated strings to/from maps
 */
public class MapStringifier {

    public static final String KVP_SEPARATOR = ",";
    public static final String KV_SEPARATOR = ":";

    /**
     * MapStringifier with default separators; "key:value,key:value"
     */
    public static final MapStringifier MAP_STRINGIFIER = new MapStringifier(KVP_SEPARATOR, KV_SEPARATOR);

    private String pairSeparator;
    private String keyValueSeparator;

    public MapStringifier() {
        this(KVP_SEPARATOR, KV_SEPARATOR);
    }

    public MapStringifier(String pairSeparator, String keyValueSeparator) {
        this.pairSeparator = pairSeparator;
        this.keyValueSeparator = keyValueSeparator;
    }

    public static MapStringifier of(String pairSeparator, String keyValueSeparator) {
        return new MapStringifier(pairSeparator, keyValueSeparator);
    }

    public <K, V> StringBuilder stringBuildify(Map<K, V> map) {
        StringBuilder sb = new StringBuilder();
        map.forEach((column, value) -> {
            if (sb.length() > 0) {
                sb.append(pairSeparator);
            }
            sb.append(column)
                    .append(keyValueSeparator)
                    .append(value);
        });
        return sb;
    }

    public <K, V> String stringify(Map<K, V> map) {
        return stringBuildify(map).toString();
    }

    public Map<String, String> mapify(String line, Map<String, String> map) {
        String[] keyValPairs = line.split(pairSeparator);
        Arrays.stream(keyValPairs).forEach(kvp -> {
            String[] keyVal = kvp.trim().split(keyValueSeparator);
            if (keyVal.length == 2) {
                map.put(keyVal[0], keyVal[1]);
            }
        });

        return map;
    }

    public Map<String, String> mapify(String line) {
        return mapify(line, new HashMap<>());
    }

    public static ElementStringify elementStringifier() {
        return new ElementStringify(KV_SEPARATOR);
    }

    /**
     * Separated key/value manipulation
     */
    public static class ElementStringify {

        public static final ElementStringify HADOOP_KEY_VAL = new ElementStringify("\t");
        public static final ElementStringify COMMA = new ElementStringify(",");

        private String separator;
        private String regex;

        public ElementStringify(String separator) {
            this(separator, null);
        }

        public ElementStringify(String separator, String regex) {
            this.separator = separator;
            if (regex == null) {
                regex = separator;
            }
            this.regex = regex;
        }

        public static ElementStringify of(String separator) {
            return new ElementStringify(separator);
        }

        public String stringifyElement(String field, String value) {
            return field+separator+value;
        }

        public Pair<String, String> destringifyElement(String element) {
            Pair<String, String> result;
            String[] splits = element.split(regex);
            if (splits.length == 2) {
                result = Pair.of(splits[0], splits[1]);
            } else {
                result = Pair.of(null, null);
            }
            return result;
        }
    }
}

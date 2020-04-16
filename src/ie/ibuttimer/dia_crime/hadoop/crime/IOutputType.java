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

package ie.ibuttimer.dia_crime.hadoop.crime;

import java.util.Map;
import java.util.TreeMap;

/**
 * Interface for output types producers
 */
public interface IOutputType {

    Map<String, OpTypeEntry> getOutputTypeMap();

    default Map<String, OpTypeEntry> newOutputTypeMap() {
        return new TreeMap<>();
    }

    default void putOutputType(String name, Class<?> cls, String src) {
        getOutputTypeMap().put(name, OpTypeEntry.of(cls, src));
    }

    default void putOutputTypes(Map<String, Object> collection, String src) {
        Map<String, OpTypeEntry> outputTypes = getOutputTypeMap();
        collection.forEach((key, value) -> outputTypes.put(key, OpTypeEntry.of(value.getClass(), src)));
    }

    String getSection();

    class OpTypeEntry {
        private Class<?> cls;
        private String src;

        public OpTypeEntry(Class<?> cls, String src) {
            this.cls = cls;
            this.src = src;
        }

        public static OpTypeEntry of(Class<?> cls, String src) {
            return new OpTypeEntry(cls, src);
        }

        public Class<?> getCls() {
            return cls;
        }

        public String getSrc() {
            return src;
        }
    }
}

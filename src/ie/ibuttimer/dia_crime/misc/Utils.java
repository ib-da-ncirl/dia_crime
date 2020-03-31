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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Utils {

    private Utils() {
        // class can't be externally instantiated
    }

    public static int getJREVersion() {
        String version = System.getProperty("java.version");
        if(version.startsWith("1.")) {
            // Java 8 or lower has 1.x.y version numbers
            version = version.substring(2, 3);
        } else {
            // Java 9 or higher has x.y.z version numbers
            int dot = version.indexOf(".");
            if (dot != -1) {
                version = version.substring(0, dot);
            }
        }
        return Integer.parseInt(version);
    }

    public static <T> List<T> iterableToList(Iterable<T> values) {
        // TODO check this, not sure it adds everything to the list
        return StreamSupport.stream(values.spliterator(), false)
            .collect(Collectors.toList());
    }

    public static <K, V, T> List<T> iterableOfMapsToList(Iterable<? extends Map<K, V>> values, Class<T> cls) {
        return streamOfMapsToList(StreamSupport.stream(values.spliterator(), false), cls);
    }

    public static <K, V, T> List<T> listOfMapsToList(List<? extends Map<K, V>> values, Class<T> cls) {
        return streamOfMapsToList(values.stream(), cls);
    }

    public static <K, V, T> List<T> streamOfMapsToList(Stream<? extends Map<K, V>> values, Class<T> cls) {
        return values
            .flatMap(m -> m.entrySet().stream())
            .map(Map.Entry::getValue)
            .filter(cls::isInstance)
            .map(cls::cast)
            .collect(Collectors.toList());
    }
}

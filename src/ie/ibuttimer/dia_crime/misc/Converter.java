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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

/**
 * A collection of converter utilities
 */
public class Converter {

    private Converter() {
        // no class instances
    }

    public static Map<String, Double> stringToDoubleMap(Map<String, String> map,
                                                        Predicate<? super Map.Entry<String, String>> predicate) {
        Map<String, Double> out = new HashMap<>();
        map.entrySet().stream()
            .filter(predicate)
            .forEach(es -> {
                out.put(es.getKey(), Double.valueOf(es.getValue()));
            });
        return out;
    }

    public static Map<String, Double> stringToDoubleMap(Map<String, String> map) {
        return stringToDoubleMap(map, es -> true);
    }

}

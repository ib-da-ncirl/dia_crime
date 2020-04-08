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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Counter for multiple values. Vales are stored for the count objects and returned as the result of high/low etc.
 * @param <K>   Class for count identifiers
 * @param <V>   Class for items to be returned as results
 */
public class Counter<K, V> {

    private Map<K, V> keyValues = new HashMap<>();
    private Map<K, Integer> keyCounts = new HashMap<>();

    public Counter() {
        keyValues = new HashMap<>();
        keyCounts = new HashMap<>();
    }

    public void reset() {
        keyValues.clear();
        keyCounts.clear();
    }

    public int inc(K key, V value) {
        int count = 0;
        if (keyCounts.containsKey(key)) {
            count = keyCounts.get(key);
        }
        ++count;
        keyCounts.put(key, count);
        keyValues.put(key, value);
        return count;
    }

    public int dec(K key) {
        int count = 0;
        if (keyCounts.containsKey(key)) {
            count = keyCounts.get(key);
        }
        keyCounts.remove(key);
        keyValues.remove(key);
        return count;
    }

    private List<V> getHighOrLow(boolean high) {
        Comparator<Map.Entry<K, Integer>> compareBy;
        if (high) {
            compareBy = Map.Entry.comparingByValue(Comparator.reverseOrder());
        } else {
            compareBy = Map.Entry.comparingByValue();
        }

        List<Map.Entry<K, Integer>> sortedCounts = keyCounts.entrySet().stream()
            .sorted(compareBy)
            .collect(Collectors.toList());

        int count = sortedCounts.get(0).getValue();
        List<V> values = keyCounts.entrySet().stream()
            .filter(e -> e.getValue() == count)
            .map(e -> keyValues.get(e.getKey()))
            .collect(Collectors.toList());

        return values;
    }

    public List<V> highest() {
        return getHighOrLow(true);
    }

    public List<V> lowest() {
        return getHighOrLow(false);
    }
}

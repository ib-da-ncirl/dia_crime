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

package ie.ibuttimer.dia_crime.hadoop.merge;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

public class ValueCache<K, VC, VR> {

    /*
        Cache for use in fill-forward operations, if a value is missing fill it with the previous value.

        The value cache contains keys and associated partial values, while the required cache contains
        keys for which a full value in not available. Combining a value from the cache with a value from the
        required cache generates a complete value.
     */
    
    private Pair<K, VC> minValue;
    private Pair<K, VC> maxValue;
    private Map<K, VC> valueCache;       // cache with keys and associated partial values
    private Map<K, VR> requiredCache;    // keys for which values is required, and corresponding partial values
    private Lock lock;
    private Comparator<K> keyComparator;
    private UnaryOperator<K> prevKeyOp;
    private UnaryOperator<K> nextKeyOp;


    public ValueCache(Comparator<K> keyComparator, UnaryOperator<K> prevKeyOp, UnaryOperator<K> nextKeyOp) {
        this.minValue = null;
        this.maxValue = null;
        this.valueCache = new HashMap<>();
        this.requiredCache = new HashMap<>();
        this.lock = new ReentrantLock();
        this.keyComparator = keyComparator;
        this.prevKeyOp = prevKeyOp;
        this.nextKeyOp = nextKeyOp;
    }

    public Triple<K, VC, VR> addCache(K key, VC value) {
        Triple<K, VC, VR> result = null;
        Pair<K, VC> entry = Pair.of(key, value);

        lock.lock();
        try {
            putValueCache(key, value);

            // remove from required
            requiredCache.remove(key);

            // new entry means entry for previous key is no longer required
            K adjacentKey = prevKeyOp.apply(key);
            valueCache.remove(adjacentKey);

            // if new entry is in required return it
            adjacentKey = nextKeyOp.apply(key);
            if (requiredCache.containsKey(adjacentKey)) {
                VR cachedValue = requiredCache.get(adjacentKey);
                result = Triple.of(adjacentKey, value, cachedValue);
            }
        } finally {
            lock.unlock();
        }

        return result;
    }

    private void putValueCache(K key, VC value) {
        valueCache.put(key, value);

        Pair<K, VC> entry = Pair.of(key, value);
        setMinValue(entry);
        setMaxValue(entry);
    }

    private void setMinValue(Pair<K, VC> value) {
        if (minValue == null) {
            minValue = value;
        } else if (keyComparator.compare(minValue.getLeft(), value.getLeft()) > 0) {
            minValue = value;
        }
    }

    private void setMaxValue(Pair<K, VC> value) {
        if (maxValue == null) {
            maxValue = value;
        } else if (keyComparator.compare(maxValue.getLeft(), value.getLeft()) < 0) {
            maxValue = value;
        }
    }

    public List<Triple<K, VC, VR>> getRequiredAsMin() {
        List<Triple<K, VC, VR>> result = new ArrayList<>();
        lock.lock();
        try {
            requiredCache.forEach((key, val) -> {
                result.add(Triple.of(key, minValue.getRight(), val));
            });
        } finally {
            lock.unlock();
        }
        return result;
    }

    public Triple<K, VC, VR> addRequired(K key, VR value) {
        Triple<K, VC, VR> result = null;
        lock.lock();
        try {
            K prevKey = prevKeyOp.apply(key);
            if (valueCache.containsKey(prevKey)) {
                // the required value is in the cache, so return it
                VC cachedValue = valueCache.get(prevKey);
                result = Triple.of(key, cachedValue, value);
                // remove old value and add new
                valueCache.put(key, valueCache.remove(prevKey));
            } else {
                // add to required cache
                requiredCache.put(key, value);
            }
        } finally {
            lock.unlock();
        }
        return result;
    }



}

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

package ie.ibuttimer.dia_crime.hadoop.regression;

import ie.ibuttimer.dia_crime.hadoop.AbstractBaseWritable;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static ie.ibuttimer.dia_crime.misc.Functional.exceptionLoggingBiConsumer;
import static ie.ibuttimer.dia_crime.misc.Utils.getLogger;

/**
 * Custom writable to store properties
 * @param <K>   Class of key
 * @param <V>   Class of value
 */
public class RegressionWritable<K, V extends Writable> extends AbstractBaseWritable<RegressionWritable<K, V>> implements Writable, Map<K, V> {

    private Map<K, V> properties;

    public RegressionWritable() {
        super();
        properties = new HashMap<>();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);

        dataOutput.writeInt(properties.size());
        properties.forEach(exceptionLoggingBiConsumer((key, value) -> {
            Text.writeString(dataOutput, key.toString());
            value.write(dataOutput);
        }, IOException.class, getLogger()));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);

        for (int size = dataInput.readInt(); size > 0; --size) {
            // TODO sort out generics, atm keys can only be String
            String key = Text.readString(dataInput);

            Value value = Value.of();
            value.readFields(dataInput);
            properties.put((K)key, (V)value);

        }
    }

    public void setProperty(K name, V value) {
        properties.put(name, value);
    }

    public V getProperty(K name) {
        return properties.get(name);
    }

    @Override
    public RegressionWritable<K, V> getInstance() {
        return new RegressionWritable<K, V>();
    }

    @Override
    public void set(RegressionWritable<K, V> other) {
        super.set(other);
        properties.putAll(other.properties);
    }

    @Override
    public RegressionWritable<K, V> copyOf() {
        RegressionWritable<K, V> copy = new RegressionWritable<K, V>();
        copy.set(this);
        return copy;
    }

    @Override
    public int size() {
        return properties.size();
    }

    @Override
    public boolean isEmpty() {
        return properties.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
        return properties.containsKey(o);
    }

    @Override
    public boolean containsValue(Object o) {
        return properties.containsValue(o);
    }

    @Override
    public V get(Object o) {
        return properties.get(o);
    }

    @Override
    public V put(K k, V v) {
        return properties.put(k, v);
    }

    @Override
    public V remove(Object o) {
        return properties.remove(o);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        properties.putAll(map);
    }

    @Override
    public void clear() {
        properties.clear();
    }

    @Override
    public Set<K> keySet() {
        return properties.keySet();
    }

    @Override
    public Collection<V> values() {
        return properties.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return properties.entrySet();
    }
}

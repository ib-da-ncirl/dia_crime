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

package ie.ibuttimer.dia_crime.hadoop.misc;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Optional;

public class CounterEnums {

    private CounterEnums() {
    }

    public interface ICounter {

        Optional<Counter> getCounter();

        default void setValue(long value) {
            getCounter().ifPresent(c -> c.setValue(value));
        }

        default void reset() {
            setValue(0);
        }

        default void incrementValue(long value) {
            getCounter().ifPresent(c -> c.increment(value));
        }

        default void increment() {
            incrementValue(1);
        }

        default Optional<Long> getCount() {
            return getCounter().map(Counter::getValue);
        }
    }

    public abstract static class AbstractCounter implements ICounter {

        protected String className;
        protected String name;

        public AbstractCounter(String className, String name) {
            this.className = className;
            this.name = name;
        }
    }

    public static class MapperCounter extends AbstractCounter {

        Mapper<?,?,?,?>.Context context;

        public MapperCounter(Mapper<?,?,?,?>.Context context, String className, String name) {
            super(className, name);
            this.context = context;
        }

        @Override
        public Optional<Counter> getCounter() {
            return Optional.of(context.getCounter(className, name));
        }
    }

    public static class ReducerCounter extends AbstractCounter {

        Reducer<?,?,?,?>.Context context;

        public ReducerCounter(Reducer<?,?,?,?>.Context context, String className, String name) {
            super(className, name);
            this.context = context;
        }

        public Optional<Counter> getCounter() {
            return Optional.of(context.getCounter(className, name));
        }
    }

}

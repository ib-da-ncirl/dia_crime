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

package ie.ibuttimer.dia_crime.hadoop.stats;

import ie.ibuttimer.dia_crime.hadoop.AbstractReducer;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

public class StatsReducer extends AbstractReducer<Text, Value, Text, Text> implements IStats {

    private Counters.ReducerCounter counter;

    private Map<String, Class<?>> outputTypes;

    private List<String> variables;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        counter = getCounter(context, CountersEnum.STATS_REDUCER_COUNT);

        Configuration conf = context.getConfiguration();
        StatsConfigReader cfgReader = new StatsConfigReader(StatsMapper.getCsvEntryMapperCfg());

        variables = cfgReader.readVariables(conf);
        outputTypes = cfgReader.readOutputTypes(conf);
    }

    @Override
    protected void reduce(Text key, Iterable<Value> values, Context context) throws IOException, InterruptedException {

        String keyStr = key.toString();
        String keyBase;
        boolean stdKey = isStandardKey(keyStr);
        if (stdKey) {
            keyBase = keyStr;
        } else {
            keyBase = splitKeyTag(keyStr).getLeft();
        }

        Triple <Value, Value, Value> collectors = initialiseCollectors(keyBase);
        Value summer = collectors.getLeft();
        Value minimiser = collectors.getMiddle();
        Value maximiser = collectors.getRight();

        AtomicLong entryCount = new AtomicLong();

        if (stdKey) {
            values.forEach(writable -> {
                summer.add(writable);
                minimiser.min(writable);
                maximiser.max(writable);

                counter.increment();
                entryCount.incrementAndGet();
            });
            writeOutput(context, Stream.of(
                Pair.of(summer, new Text(getSumKeyTag(key.toString()))),
                Pair.of(minimiser, new Text(getMinKeyTag(key.toString()))),
                Pair.of(maximiser, new Text(getMaxKeyTag(key.toString())))
            ));
        } else {
            // slight duplication but min/min not required for squared values and it'll be quicker
            values.forEach(writable -> {
                summer.add(writable);

                counter.increment();
                entryCount.incrementAndGet();
            });
            writeOutput(context, Stream.of(
                Pair.of(summer, key)
            ));
        }

        // write count to file
        Map<String, String> map = Map.of(COUNT_PROP, Long.toString(entryCount.get()));
        writeOutput(context, new Text(getCountKeyTag(key.toString())), new Text(MapStringifier.stringify(map)));
    }

    private void writeOutput(Context context, Stream<Pair<Value, Text>> stream) {
        Map<String, String> map = new TreeMap<>();

        stream.forEach(pair -> {
                writeOutput(context, pair.getRight(), new Text(pair.getLeft().value().toString()));
        });
    }

    private void writeOutput(Context context, Text key, Text value) {
        try {
            context.write(key, value);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    private Triple<Value, Value, Value> initialiseCollectors(String field) {

        AtomicReference<Value> summer = new AtomicReference<>();
        AtomicReference<Value> minimiser = new AtomicReference<>();
        AtomicReference<Value> maximiser = new AtomicReference<>();
        outputTypes.entrySet().stream()
            .filter(es -> es.getKey().equals(field))
            .findFirst()
            .ifPresent(es -> {
                Class<?> cls = es.getValue();
                Object sum = null;
                Object min = null;
                Object max = null;
                if (cls.equals(BigInteger.class)) {
                    sum = BigInteger.ZERO;
                    min = Value.MAX_BIG_INTEGER;
                    max = Value.MIN_BIG_INTEGER;
                } else if (cls.equals(BigDecimal.class)) {
                    sum = BigDecimal.ZERO;
                    min = Value.MAX_BIG_DECIMAL;
                    max = Value.MIN_BIG_DECIMAL;
                } else if (cls.equals(Double.class)) {
                    sum = 0.0;
                    min = Double.MAX_VALUE;
                    max = Double.MIN_VALUE;
                } else if (cls.equals(Float.class)) {
                    sum = 0.0;
                    min = Float.MAX_VALUE;
                    max = Float.MIN_VALUE;
                } else if (cls.equals(Long.class)) {
                    sum = 0;
                    min = Double.MAX_VALUE;
                    max = Double.MIN_VALUE;
                } else if (cls.equals(Integer.class)) {
                    sum = 0;
                    min = Integer.MAX_VALUE;
                    max = Integer.MIN_VALUE;
                }

                if (sum != null) {
                    summer.set(Value.of(sum));
                    minimiser.set(Value.of(min));
                    maximiser.set(Value.of(max));
                }
            });
        return Triple.of(summer.get(), minimiser.get(), maximiser.get());
    }
}

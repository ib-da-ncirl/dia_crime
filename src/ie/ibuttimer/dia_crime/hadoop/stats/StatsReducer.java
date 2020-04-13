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
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Reducer for statistics, which accumulates values to output sum of values
 * - input key : property name plus specific identifier for squared value etc.
 * - input value : value
 * - output key : property name plus specific identifier for the statistic
 * - output value : value
 */
public class StatsReducer extends AbstractReducer<Text, Value, Text, Text> {

    private Counters.ReducerCounter counter;
    private Counters.ReducerCounter statsInCounter;
    private Counters.ReducerCounter statsOutCounter;

    private Map<String, Class<?>> outputTypes;

    private List<String> variables;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        counter = getCounter(context, CountersEnum.STATS_REDUCER_COUNT);
        statsInCounter = getCounter(context, CountersEnum.STATS_REDUCER_GROUP_IN_COUNT);
        statsOutCounter = getCounter(context, CountersEnum.STATS_REDUCER_GROUP_OUT_COUNT);

        Configuration conf = context.getConfiguration();
        StatsConfigReader cfgReader = new StatsConfigReader(StatsMapper.getClsCsvMapperCfg());

        variables = cfgReader.readVariables(conf);
        outputTypes = cfgReader.readOutputTypes(conf);
    }

    @Override
    protected void reduce(Text key, Iterable<Value> values, Context context) throws IOException, InterruptedException {

        Optional<Long> inCount = statsInCounter.getCount();
        inCount.ifPresent(count -> {
            if (count == 0) {
                Configuration conf = context.getConfiguration();

                getTagStrings(conf, STATS_PROP_SECTION).forEach(tagLine -> {
                    try {
                        context.write(new Text(COMMENT_PREFIX), new Text(tagLine));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            }
        });

        statsInCounter.increment();

        String keyStr = key.toString();
        Triple<Optional<String>, Optional<String>, Optional<String>> keySplit = NameTag.split(keyStr);

        keySplit.getLeft().ifPresent(key1 -> {

            Triple <Value, Value, Value> collectors = initialiseCollectors(key1);
            Value summer = collectors.getLeft();
            Value zeroCnt = Value.of(0L);

            AtomicLong entryCount = new AtomicLong(0);
            List<Pair<Text, Value>> outputList = new ArrayList<>();

            if (NameTag.isStandardKey(keyStr)) {
                Value minimiser = collectors.getMiddle();
                Value maximiser = collectors.getRight();

                values.forEach(writable -> {
                    reduceStd(writable, summer, zeroCnt);
                    minimiser.min(writable);
                    maximiser.max(writable);

                    entryCount.incrementAndGet();
                });
                /* output following key/values:
                    <key>-MIN - min value
                    <key>-MAX - max value
                 */
                outputList.addAll(List.of(
                    Pair.of(new Text(NameTag.MIN.getKeyTag(keyStr)), minimiser),
                    Pair.of(new Text(NameTag.MAX.getKeyTag(keyStr)), maximiser)
                ));
            } else {
                // slight duplication but min/min not required for squared/product values and it'll be quicker
                values.forEach(writable -> {
                    reduceStd(writable, summer, zeroCnt);

                    entryCount.incrementAndGet();
                });
            }
            counter.incrementValue(entryCount.get());

            /* always output following key/values:
                <key>-SUM - sum of values
                <key>-CNT - count of values
                <key>-ZERO - count of zero values
                <key>-MEAN - mean value
             */
            Value count = Value.of(entryCount.get());
            Value mean = summer.copyOf();
            mean.divide(count);

            outputList.addAll(List.of(
                Pair.of(new Text(NameTag.SUM.getKeyTag(keyStr)), summer),
                Pair.of(new Text(NameTag.CNT.getKeyTag(keyStr)), count),
                Pair.of(new Text(NameTag.ZERO.getKeyTag(keyStr)), zeroCnt),
                Pair.of(new Text(NameTag.MEAN.getKeyTag(keyStr)), mean)
            ));

            writeOutput(context, outputList);
        });
    }

    private void reduceStd(Value value, Value summer, Value zeroCnt) {

        summer.add(value);

        value.asDouble(d -> {
            if (d == 0.0) {
                zeroCnt.add(1);
            }
        });
    }

    private void writeOutput(Context context, List<Pair<Text, Value>> stream) {
        stream.forEach(pair -> {
            writeOutput(context, pair.getLeft(), new Text(pair.getRight().value().toString()));
        });
    }

    private void writeOutput(Context context, Text key, Text value) {
        try {
            context.write(key, value);

            statsOutCounter.increment();
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
                    sum = (double) 0;
                    min = Double.MAX_VALUE;
                    max = Double.MIN_VALUE;
                } else if (cls.equals(Float.class)) {
                    sum = (double) 0;
                    min = Float.MAX_VALUE;
                    max = Float.MIN_VALUE;
                } else if (cls.equals(Long.class)) {
                    sum = (double) 0;
                    min = Double.MAX_VALUE;
                    max = Double.MIN_VALUE;
                } else if (cls.equals(Integer.class)) {
                    sum = (double) 0;
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

    @Override
    protected Text newKey(String key) {
        return new Text(key);
    }

    @Override
    protected Text newValue(String value) {
        return new Text(value);
    }
}

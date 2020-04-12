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

import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.stats.NameTag;
import ie.ibuttimer.dia_crime.misc.DebugLevel;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import ie.ibuttimer.dia_crime.misc.Utils;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Reducer for regression validation
 * - input key : the current epoch number
 * - input value : RegressionWritable containing individual values
 * - output key : the current epoch number
 * - output value : the current model
 */
public class RegressionTrainReducer extends AbstractRegressionReducer<Text, RegressionWritable<String, Value>, Text, Text> {

    private Counters.ReducerCounter counter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        setSection(REGRESSION_PROP_SECTION);

        super.setup(context);

        counter = getCounter(context, CountersEnum.REGRESSION_REDUCER_COUNT);
    }

    @Override
    protected void reduce(Text key, Iterable<RegressionWritable<String, Value>> values, Context context) throws IOException, InterruptedException {

        addOutputHeader(context, counter, List.of(TRAIN_START_DATE_PROP, TRAIN_END_DATE_PROP), List.of());

        Value sqErrorSummer = Value.of(0.0);
        Value pdWeightSummer = Value.of(0.0);
        Value pdBiasSummer = Value.of(0.0);
        Value countRef = Value.of(0L);

        String errSqTag = NameTag.getKeyTagChain("", List.of(NameTag.ERR, NameTag.SQ));

        values.forEach(writable-> {

            writable.forEach((name, value) -> {

                if (name.endsWith(errSqTag)) {
                    sqErrorSummer.add(value);
                } else if (NameTag.PDW.is(name)) {
                    pdWeightSummer.add(value);
                } else if (NameTag.PDB.is(name)) {
                    pdBiasSummer.add(value);
                } else if (NameTag.CNT.is(name)) {
                    countRef.set(value);
                }

                counter.increment();
            });
            if (show(DebugLevel.HIGH)) {
                StringBuilder sb = new StringBuilder()
                    .append("sqErr=").append(sqErrorSummer.doubleValue()).append(',')
                    .append("pdWeight=").append(pdWeightSummer.doubleValue()).append(',')
                    .append("pdBias=").append(pdBiasSummer.doubleValue()).append(',')
                    .append("count=").append(countRef.longValue());
                getLogger().info(key.toString() + " " + sb.toString());
            }

        });

        long count = countRef.longValue();
        double cost = regressor.cost(sqErrorSummer.doubleValue(), count);
        Pair<Double, Double> updated = regressor.calcUpdatedWeights(
            pdWeightSummer.doubleValue(), pdBiasSummer.doubleValue(), count);

        Map<String, Double> result = new HashMap<>();
        result.put(WEIGHT_PROP, updated.getLeft());
        result.put(BIAS_PROP, updated.getRight());
        result.put("cost", cost);

        getLogger().info(
            Utils.getSpacedDialog(
                List.of("Regression Result",
                    String.format("%s - %s", key.toString(), result))));

        context.write(key, new Text(MapStringifier.stringify(result)));
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

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
import ie.ibuttimer.dia_crime.misc.Utils;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.MAP_STRINGIFIER;

/**
 * Reducer for regression validation
 * - input key : dependent variable
 * - input value : RegressionWritable containing values for y and y-hat
 * - output key : dependent variable
 * - output value : model verification result
 */
public class RegressionValidateReducer extends AbstractRegressionReducer<Text, RegressionWritable<String, Value>, Text, Text> {

    private Counters.ReducerCounter counter;

    private List<CacheEntry> yYhatCache;

    private Text keyOut = new Text(COMMENT_PREFIX);
    private Text valueOut = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        setSection(VERIFICATION_PROP_SECTION);

        super.setup(context);

        counter = getCounter(context, CountersEnum.REGRESSION_REDUCER_COUNT);

        yYhatCache = new ArrayList<>();
    }

    @Override
    protected void reduce(Text key, Iterable<RegressionWritable<String, Value>> values, Context context) throws IOException, InterruptedException {

        addOutputHeader(context, counter, List.of(VALIDATE_START_DATE_PROP, VALIDATE_END_DATE_PROP), List.of(
            String.format("model : %s", regressor)
        ));

        Value summer = Value.of(0.0);
        Value regSummer = Value.of(0.0);    // SSR
        Value errSummer = Value.of(0.0);    // SSE
        Value totalSummer = Value.of(0.0);  // SST

        String yhatTag = NameTag.YHAT.getKeyTag(dependent);

        AtomicInteger numVariables = new AtomicInteger(0);
        values.forEach(writable-> {
            if (writable.containsKey(dependent) && writable.containsKey(yhatTag)) {

                // num independents is writable size minus dependent & yhat entries
                numVariables.set(writable.size() - 2);

                double yi = writable.get(dependent).doubleValue();
                double yhati = writable.get(yhatTag).doubleValue();

                yYhatCache.add(CacheEntry.of(yi, yhati));

                counter.increment();

                summer.add(yi);
            }
        });

        // calc mean of sample
        summer.divide(yYhatCache.size());
        double mean = summer.doubleValue();

        StringBuilder sb = new StringBuilder();

        // calc stats
        yYhatCache.forEach(pair -> {
            double ssr = regressor.regressionSum(pair.yhat, mean);
            double sse = regressor.errorSum(pair.y, pair.yhat);
            double sst = regressor.totalSum(pair.y, mean);
            regSummer.add(ssr);
            errSummer.add(sse);
            totalSummer.add(sst);

            sb.delete(0, sb.length());
            valueOut.set(
                sb.append("yi=").append(pair.y)
                    .append(",yhati=").append(pair.yhat)
                    .append(",ssr=").append(ssr)
                    .append(",sse=").append(sse)
                    .append(",sst=").append(sst)
                    .toString());
            try {
                context.write(keyOut, valueOut);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        Map<String, Double> result = new HashMap<>();
        double rSquared = regressor.calcRSquared(regSummer.doubleValue(), totalSummer.doubleValue());
        double rBarSquared = regressor.calcRBarSquared(rSquared, yYhatCache.size(), numVariables.get());
        double stdError = regressor.stdErrorOfRegression(errSummer.doubleValue(), yYhatCache.size(), numVariables.get());

        result.put("r_squared", rSquared);
        result.put("r_bar_squared", rBarSquared);
        result.put("regression_std_err", stdError);

        getLogger().info(
            Utils.getSpacedDialog(
                List.of("Regression Verification Result",
                    String.format("%s - %s", dependent, result))));

        valueOut.set(MAP_STRINGIFIER.stringify(result));
        context.write(key, valueOut);
    }

    static class CacheEntry {

        public final Double y;
        public final Double yhat;

        public CacheEntry(Double y, Double yhat) {
            this.y = y;
            this.yhat = yhat;
        }

        public static CacheEntry of(Double y, Double yhat) {
            return new CacheEntry(y, yhat);
        }
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

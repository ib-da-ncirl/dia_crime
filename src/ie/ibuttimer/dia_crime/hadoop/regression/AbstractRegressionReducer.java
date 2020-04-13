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

import ie.ibuttimer.dia_crime.hadoop.AbstractReducer;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.stats.StatsConfigReader;
import ie.ibuttimer.dia_crime.misc.ConfigReader;
import ie.ibuttimer.dia_crime.misc.DebugLevel;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

public abstract class AbstractRegressionReducer<KI, VI, KO, VO> extends AbstractReducer<KI, VI, KO, VO> {

    protected List<String> independents;
    protected String dependent;

    protected LinearRegressor regressor;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (getSection() == null) {
            throw new IllegalStateException("Reducer 'section' not set");
        }

        super.setup(context);

        Configuration conf = context.getConfiguration();
        StatsConfigReader cfgReader = new StatsConfigReader(getSection());

        independents = cfgReader.readCommaSeparatedProperty(conf, INDEPENDENTS_PROP);
        dependent = cfgReader.getConfigProperty(conf, DEPENDENT_PROP);

        double weight = cfgReader.getConfigProperty(conf, WEIGHT_PROP, (double) 0).doubleValue();
        double bias = cfgReader.getConfigProperty(conf, BIAS_PROP, (double) 0).doubleValue();
        double learningRate = cfgReader.getConfigProperty(conf, LEARNING_RATE_PROP, (double) 0).doubleValue();

        regressor = new LinearRegressor(weight, bias, learningRate);

        if (show(DebugLevel.HIGH)) {
            getLogger().info(regressor.toString());
        }
    }

    protected void addOutputHeader(Context context, Counters.ReducerCounter counter, List<String> additionalTags,
                                   List<String> rawStrings) {

        List<String> additionalProps = new ArrayList<>(List.of(INDEPENDENTS_PROP, DEPENDENT_PROP));
        additionalProps.addAll(additionalTags);

        super.addOutputHeader(context, counter, additionalProps, rawStrings);
    }
//    protected void addOutputHeader(Context context, Counters.ReducerCounter counter, List<String> additionalTags,
//                                   List<String> rawStrings) {
//        Optional<Long> inCount = counter.getCount();
//        inCount.ifPresent(count -> {
//            if (count == 0) {
//                Configuration conf = context.getConfiguration();
//                ConfigReader cfgReader = new ConfigReader(getSection());
//                List<String> tags = new ArrayList<>(getTagStrings(conf, getSection()));
//
//                List<String> props = new ArrayList<>(List.of(INDEPENDENTS_PROP, DEPENDENT_PROP));
//                props.addAll(additionalTags);
//
//                props.forEach(p -> tags.add(
//                        String.format("%s : %s", p, cfgReader.getConfigProperty(conf, p, ""))));
//
//                tags.addAll(rawStrings);
//
//                tags.forEach(tagLine -> {
//                    try {
//                        context.write(newKey(COMMENT_PREFIX), newValue(tagLine));
//                    } catch (IOException | InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//        });
//    }
//
//    protected abstract KO newKey(String key);
//
//    protected abstract VO newValue(String value);
}

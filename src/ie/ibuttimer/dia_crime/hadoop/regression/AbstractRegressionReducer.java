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
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static ie.ibuttimer.dia_crime.hadoop.regression.AbstractRegressionMapper.REGRESSOR;
import static ie.ibuttimer.dia_crime.hadoop.regression.AbstractRegressionMapper.getRegressionSetup;
import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Base regression reducer class
 * @param <KI>  Reducer key input class
 * @param <VI>  Reducer values input class
 * @param <KO>  Reducer key output class
 * @param <VO>  Reducer values output class
 */
public abstract class AbstractRegressionReducer<KI, VI, KO, VO> extends AbstractReducer<KI, VI, KO, VO> {

    protected List<String> independents;
    protected String dependent;

    protected LinearRegressor regressor;

    @Override
    @SuppressWarnings("unchecked")
    protected void setup(Context context) throws IOException, InterruptedException {
        if (getSection() == null) {
            throw new IllegalStateException("Reducer 'section' not set");
        }

        super.setup(context);

        Configuration conf = context.getConfiguration();
        StatsConfigReader cfgReader = new StatsConfigReader(getSection());

        Map<String, Object> regressionSetup = getRegressionSetup(conf, cfgReader,
                                                RegressionTrainMapper.getClsCsvMapperCfg(), this);

        independents = (List<String>) regressionSetup.get(INDEPENDENTS_PROP);
        dependent = (String) regressionSetup.get(DEPENDENT_PROP);
        regressor = (LinearRegressor) regressionSetup.get(REGRESSOR);

        List<String> missing = regressor.getMissingCoefficients(independents);
        if (missing.size() > 0) {
            throw new IllegalArgumentException("Missing weight setting for " + missing);
        }
    }

    @Override
    protected void addOutputHeader(Context context, Counters.ReducerCounter counter, List<String> additionalTags,
                                   List<String> rawStrings) {

        List<String> additionalProps = new ArrayList<>(List.of(INDEPENDENTS_PROP, DEPENDENT_PROP));
        additionalProps.addAll(additionalTags);

        super.addOutputHeader(context, counter, additionalProps, rawStrings);
    }
}

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

import ie.ibuttimer.dia_crime.hadoop.AbstractCsvMapper;
import ie.ibuttimer.dia_crime.hadoop.ICsvMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.ITagger;
import ie.ibuttimer.dia_crime.hadoop.io.FileReader;
import ie.ibuttimer.dia_crime.hadoop.stats.NameTag;
import ie.ibuttimer.dia_crime.hadoop.stats.StatsConfigReader;
import ie.ibuttimer.dia_crime.misc.DebugLevel;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.ElementStringify.HADOOP_KEY_VAL;

/**
 * Base regression mapper class
 * - input key : csv file line number
 * - input value : csv file line text
 * - output value : RegressionWritable containing individual values
 * @param <K>   output key type
 * @param <R>   output key type of RegressionWritable
 * @param <V>   output value type of RegressionWritable
 */
public abstract class AbstractRegressionMapper<K, R, V extends Writable> extends AbstractCsvMapper<K, RegressionWritable<R, V>> {

    protected List<String> independents;
    protected String dependent;
    protected List<String> allVariables;

    protected Map<String, Class<?>> outputTypes;

    protected Map<String, Double> counts;

    protected LinearRegressor regressor;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        setLogger(getClass());

        super.setup(context);
        super.initIndices(context, getEntryMapperCfg().getPropertyIndices());

        Configuration conf = context.getConfiguration();
        StatsConfigReader cfgReader = new StatsConfigReader(getEntryMapperCfg());

        // TODO numeric variables added to config, unnecessary setting properties file, but need a better way
        conf.set(getEntryMapperCfg().getPropertyPath(VARIABLES_PROP), VARIABLES_NUMERIC);
        outputTypes = cfgReader.readOutputTypes(conf);

        independents = cfgReader.readCommaSeparatedProperty(conf, INDEPENDENTS_PROP);
        dependent = getConfigProperty(conf, DEPENDENT_PROP);

        allVariables = new ArrayList<>(independents);
        allVariables.add(dependent);

        counts = readCounts(getConfigProperty(conf, STATS_INPUT_PATH_PROP), conf, independents);

        double weight = cfgReader.getConfigProperty(conf, WEIGHT_PROP, (double) 0).doubleValue();
        double bias = cfgReader.getConfigProperty(conf, BIAS_PROP, (double) 0).doubleValue();
        double learningRate = cfgReader.getConfigProperty(conf, LEARNING_RATE_PROP, (double) 0).doubleValue();
        regressor = new LinearRegressor(weight, bias, learningRate);

        if (show(DebugLevel.HIGH)) {
            getLogger().info(regressor.toString());
        }
    }

    /**
     * Filter an input line, checking header and comments
     * @param key
     * @param value
     * @param context
     * @return  true if line can be processed
     */
    public FilterResult filter(LongWritable key, Text value, Context context) {
        FilterResult filterRes = null;
        if (!skipHeader(key)) {
            Pair<String, String> hKeyVal = HADOOP_KEY_VAL.destringifyElement(value.toString());
            boolean pass = !skipComment(value);
            if (!pass) {
                // verify parameters specified in input file
                ICsvMapperCfg cfg = getEntryMapperCfg();

                cfg.verifyTags(context.getConfiguration(), cfg, hKeyVal.getRight(), ITagger.DateRangeMode.WITHIN);
                /* 2001-01-02	02:3, 03:35, 04A:15, 04B:21, 05:68, 06:221, 07:65, 08A:51, 08B:122, 10:9, 11:65, 12:2, 13:2,
                    14:118, 15:9, 16:11, 17:7, 18:156, 19:1, 20:3, 22:2, 24:2, 26:155, DJI_adjclose:10646.150391,
                    DJI_close:10646.150391, DJI_date:2001-01-02, DJI_high:10797.019531, DJI_low:10585.360352,
                    DJI_open:10790.919922, DJI_volume:253300000, GSPC_adjclose:1283.27002, GSPC_close:1283.27002,
                    GSPC_date:2001-01-02, GSPC_high:1320.280029, GSPC_low:1276.050049, GSPC_open:1320.280029,
                    GSPC_volume:1129400000, IXIC_adjclose:2291.860107, IXIC_close:2291.860107, IXIC_date:2001-01-02,
                    IXIC_high:2474.159912, IXIC_low:2273.070068, IXIC_open:2474.159912, IXIC_volume:1918930000,
                    clouds_all:8, date:2001-01-02, feels_like:-15.236249, humidity:72, pressure:1034, rain_1h:0.0,
                    rain_3h:0.0, snow_1h:0.0, snow_3h:0.0, temp:-9.0183325, temp_max:-6.05125, temp_min:-11.631249,
                    total:1143, weather_description:sky is clear, weather_id:800, weather_main:Clear, wind_deg:277,
                    wind_speed:4.224999
                 */
            }
            filterRes = new FilterResult(pass, hKeyVal.getLeft(), hKeyVal.getRight()); // don't pass comment
        }
        if (filterRes == null) {
            filterRes = FilterResult.FAIL;
        }
        return filterRes;
    }

    /**
     * Filter an input line, checking header, comments and tags
     * @param key
     * @param value
     * @param context
     * @return  true if line can be processed
     */
    public FilterResult filterDate(LongWritable key, Text value, Context context) {
        FilterResult filterRes = filter(key, value, context);
        if (filterRes.pass) {
            Pair<Boolean, LocalDate> dateFilter = getDateAndFilter(filterRes.key);
            filterRes = new FilterResult(dateFilter.getLeft(), dateFilter.getRight(), filterRes.value);
        }
        return filterRes;
    }


    protected RegressionWritable<String, Value> collectValues(String value) {
        RegressionWritable<String, Value> entry = new RegressionWritable<>();

        Map<String, String> map = MapStringifier.mapify(value);

        // collect the value for each property
        outputTypes.entrySet().stream()
            .filter(es -> allVariables.stream().anyMatch(v -> v.equals(es.getKey())))
            .forEach(es -> {
                String name = es.getKey();
                Class<?> cls = es.getValue();
                String readValue = map.getOrDefault(name, Value.getDefaultValueStr(cls));

                Value wrapped = Value.of(readValue, cls, getDateTimeFormatter(), getLogger());

                entry.put(name, wrapped);
            });
        return entry;
    }

    /**
     * Read the feature counts from the stats output
     * @param statsPath
     * @param conf
     * @param prefixes
     * @return
     */
    protected Map<String, Double> readCounts(String statsPath, Configuration conf, List<String> prefixes) {
        FileReader fileReader = new FileReader(statsPath, conf);
        Map<String, Double> counts = new HashMap<>();

        try {
            List<String> allCounts = fileReader.open()
                .getAllLines(l -> l.matches(NameTag.CNT.getKeyTag("\\w+") + ".*"));
            allCounts.stream()
                .filter(l -> prefixes.stream().anyMatch(Objects.requireNonNull(l)::startsWith))
                .forEach(l -> {
                    Pair<String, String> keyVal = HADOOP_KEY_VAL.destringifyElement(l);
                    counts.put(NameTag.splitKeyTag(keyVal.getLeft()).getLeft(), Double.parseDouble(keyVal.getRight()));
                });
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            fileReader.close();
        }
        return counts;
    }


    protected static class RegressionMapperCfg extends AbstractCsvMapperCfg {

        private static final Property trainPathProp = Property.of(TRAIN_OUTPUT_PATH_PROP, "path to output training file", "");
        private static final Property indoProp = Property.of(INDEPENDENTS_PROP, "list of independent variables to use", "");
        private static final Property learningProp = Property.of(LEARNING_RATE_PROP, "learning rate to use for gradient descent", "");
        private static final Property weightProp = Property.of(WEIGHT_PROP, "weight for regression calculation", "");
        private static final Property biasProp = Property.of(BIAS_PROP, "bias for regression calculation", "");

        private static final List<Property> required = List.of(trainPathProp, indoProp, learningProp,
                                                                weightProp, biasProp);

        public RegressionMapperCfg(String propertyRoot) {
            super(propertyRoot);
        }

        @Override
        public List<Property> getAdditionalProps() {
            List<Property> list = new ArrayList<>(required);
            list.addAll(getPropertyList(List.of(FACTOR_PROP)));
            return list;
        }

        @Override
        public List<Property> getRequiredProps() {
            List<Property> list = new ArrayList<>(super.getRequiredProps());
            list.addAll(getPropertyList(List.of(STATS_INPUT_PATH_PROP, OUTPUTTYPES_PATH_PROP, DEPENDENT_PROP)));
            list.addAll(required);
            return list;
        }

        @Override
        public List<String> getPropertyIndices() {
            return List.of();
        }
    };

    static class FilterResult {
        boolean pass;
        LocalDate date;
        String key;
        String value;

        public FilterResult() {
            this(false, (String)null, null);
        }

        public FilterResult(boolean pass, String key, String value) {
            this.pass = pass;
            this.date = null;
            this.key = key;
            this.value = value;
        }

        public FilterResult(boolean pass, LocalDate date, String value) {
            this.pass = pass;
            this.date = date;
            this.key = null;
            this.value = value;
        }

        static final FilterResult FAIL = new FilterResult();
    }
}

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
import ie.ibuttimer.dia_crime.hadoop.crime.IOutputType;
import ie.ibuttimer.dia_crime.hadoop.io.FileReader;
import ie.ibuttimer.dia_crime.hadoop.stats.NameTag;
import ie.ibuttimer.dia_crime.hadoop.stats.StatsConfigReader;
import ie.ibuttimer.dia_crime.misc.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.Converter.stringToDoubleMap;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.ElementStringify.HADOOP_KEY_VAL;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.MAP_STRINGIFIER;

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

    public static final String REGRESSOR = "regressor";

    protected List<String> independents;
    protected String dependent;
    protected List<String> allVariables;

    protected Map<String, IOutputType.OpTypeEntry> outputTypes;

    protected Map<String, Double> counts;

    protected LinearRegressor regressor;

    @Override
    @SuppressWarnings("unchecked")
    protected void setup(Context context) throws IOException, InterruptedException {
        setLogger(getClass());

        ICsvMapperCfg mapperCfg = getMapperCfg();

        super.setup(context);
        super.initIndices(context, mapperCfg.getPropertyIndices());

        Configuration conf = context.getConfiguration();
        StatsConfigReader cfgReader = new StatsConfigReader(mapperCfg);

        // TODO numeric variables added to config, unnecessary setting properties file, but need a better way
        conf.set(getMapperCfg().getPropertyPath(VARIABLES_PROP), VARIABLES_NUMERIC);
        outputTypes = cfgReader.readOutputTypes(conf);

        Map<String, Object> regressionSetup = getRegressionSetup(conf, cfgReader, mapperCfg, this);

        independents = (List<String>) regressionSetup.get(INDEPENDENTS_PROP);
        dependent = (String) regressionSetup.get(DEPENDENT_PROP);
        regressor = (LinearRegressor) regressionSetup.get(REGRESSOR);

        allVariables = new ArrayList<>(independents);
        allVariables.add(dependent);

        counts = readCounts(getConfigProperty(conf, STATS_INPUT_PATH_PROP, getMapperCfg()), conf, independents);
    }

    public static final String WEIGHT_SEPARATOR = "/";
    public static final String WEIGHT_KV_SEPARATOR = "=";

    /**
     * Read regression setup
     * @param conf
     * @param cfgReader
     * @param mapperCfg
     * @param debuggable
     * @return  Map holding regressor, independents and dependent
     */
    @SuppressWarnings("unchecked")
    protected static Map<String, Object> getRegressionSetup(Configuration conf, ConfigReader cfgReader,
                                                            ICsvMapperCfg mapperCfg, DebugLevel.Debuggable debuggable) {

        Map<String, Object> result = getRegressionSetting(conf, cfgReader, mapperCfg);

        LinearRegressor regressor = new LinearRegressor(
            (Map<String, Double>) result.get(WEIGHT_PROP),
            (Double) result.get(BIAS_PROP), (Double) result.get(LEARNING_RATE_PROP));

        result.put(REGRESSOR, regressor);

        if (debuggable.show(DebugLevel.HIGH)) {
            getLogger().info(regressor.toString());
        }
        return result;
    }

    /**
     * Read regression setup
     * @param conf
     * @param cfgReader
     * @param mapperCfg
     * @return  Map holding regressor, independents and dependent
     */
    private static Map<String, Object> getRegressionSetting(Configuration conf, ConfigReader cfgReader,
                                                            ICsvMapperCfg mapperCfg) {
        Map<String, Object> result = new HashMap<>();

        List<String> independents = cfgReader.readCommaSeparatedProperty(conf, INDEPENDENTS_PROP);
        String dependent = getConfigProperty(conf, DEPENDENT_PROP, mapperCfg, cfgReader);

        result.put(INDEPENDENTS_PROP, independents);
        result.put(DEPENDENT_PROP, dependent);

        Map<String, Double> coefficients = stringToDoubleMap(
            cfgReader.readSeparatedKeyValueProperty(conf, WEIGHT_PROP, WEIGHT_SEPARATOR, WEIGHT_KV_SEPARATOR)
        );
        result.put(WEIGHT_PROP, coefficients);

        List.of(BIAS_PROP, LEARNING_RATE_PROP).forEach(prop -> {
            double value = cfgReader.getConfigProperty(conf, prop, (double) 0).doubleValue();
            result.put(prop, value);
        });
        return result;
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
                ICsvMapperCfg cfg = getMapperCfg();

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

        Map<String, String> map = MAP_STRINGIFIER.mapify(value);

        // collect the value for each property
        outputTypes.entrySet().stream()
            .filter(es -> allVariables.stream().anyMatch(v -> v.equals(es.getKey())))
            .forEach(es -> {
                String name = es.getKey();
                Class<?> cls = es.getValue().getCls();
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
        @SuppressWarnings("unchecked")
        public Pair<Integer, List<String>> checkConfiguration(Configuration conf) {
            Pair<Integer, List<String>> chkRes = super.checkConfiguration(conf);
            int resultCode = chkRes.getLeft();
            List<String> errors = new ArrayList<>(chkRes.getRight());

            ConfigReader cfgReader = new ConfigReader(this);
            Map<String, Object> settings = getRegressionSetting(conf, cfgReader, this);
            List<String> independents = (List<String>) settings.get(INDEPENDENTS_PROP);
            Map<String, Double> coefficients = (Map<String, Double>) settings.get(WEIGHT_PROP);

            if (coefficients.size() == 0) {
                String coeff = cfgReader.getConfigProperty(conf, WEIGHT_PROP);
                try {
                    double setting = Double.parseDouble(coeff);
                    Map<String, Double> map = new HashMap<>();
                    independents.forEach(i -> {
                        map.put(i, setting);
                    });

                    conf.set(cfgReader.getPropertyPath(WEIGHT_PROP),
                        MapStringifier.of(WEIGHT_SEPARATOR, WEIGHT_KV_SEPARATOR).stringify(map));

                    // reload settings
                    settings = getRegressionSetting(conf, cfgReader, this);
                    coefficients = (Map<String, Double>) settings.get(WEIGHT_PROP);

                } catch (NumberFormatException nfe) {
                    getLogger().warn("Unable to interpret " + WEIGHT_PROP + " setting: " + coeff);
                }
            }

            if (independents.size() != coefficients.size()) {
                errors.add("Error: independents/coefficients size mismatch");
                resultCode = ECODE_CONFIG_ERROR;
            }
            Map<String, Double> finalCoefficients = coefficients;
            List<String> missing = independents.stream()
                .filter(prop -> !finalCoefficients.containsKey(prop))
                .collect(Collectors.toList());
            if (missing.size() > 0) {
                errors.add("Error: coefficients missing for " + missing);
                resultCode = ECODE_CONFIG_ERROR;
            }
            missing = coefficients.keySet().stream()
                .filter(prop -> !independents.contains(prop))
                .collect(Collectors.toList());
            if (missing.size() > 0) {
                errors.add("Error: coefficients not listed in independents " + missing);
                resultCode = ECODE_CONFIG_ERROR;
            }

            return Pair.of(resultCode, errors);
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

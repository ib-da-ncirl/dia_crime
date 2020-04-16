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

package ie.ibuttimer.dia_crime.hadoop.normalise;

import ie.ibuttimer.dia_crime.hadoop.AbstractCsvMapper;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.ICsvMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.ITagger;
import ie.ibuttimer.dia_crime.hadoop.io.FileReader;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.misc.DateWritable;
import ie.ibuttimer.dia_crime.hadoop.regression.RegressionWritable;
import ie.ibuttimer.dia_crime.hadoop.stats.NameTag;
import ie.ibuttimer.dia_crime.hadoop.stats.StatsConfigReader;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static ie.ibuttimer.dia_crime.hadoop.merge.MergeReducer.*;
import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.ElementStringify.COMMA;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.ElementStringify.HADOOP_KEY_VAL;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.MAP_STRINGIFIER;

/**
 * Statistics mapper that outputs property value, property value squared and, property product values
 * - input key : file line number
 * - input value : file line text
 * - output key : date
 * - output value : normalised file line text
 */
public class NormaliseMapper extends AbstractCsvMapper<DateWritable, RegressionWritable<String, Value>> {

    private Counters.MapperCounter counter;

    private Map<String, OpTypeEntry> outputTypes;

    private List<String> variables;

    private Map<String, Double> stats;

    private final RegressionWritable<String, Value> valuesOut = new RegressionWritable<>();
    private final Map<DateWritable, RegressionWritable<String, Value>> outputList = new TreeMap<>();

    private boolean wroteTypes;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        setLogger(getClass());

        super.setup(context);
        super.initIndices(context, sCfgChk.getPropertyIndices());

        counter = getCounter(context, CountersEnum.NORMALISE_MAPPER_COUNT);

        Configuration conf = context.getConfiguration();
        StatsConfigReader cfgReader = new StatsConfigReader(getMapperCfg());

        variables = cfgReader.readVariables(conf);
        outputTypes = cfgReader.readOutputTypes(conf);

        stats = readStats(cfgReader.getConfigProperty(conf, STATS_INPUT_PATH_PROP), conf,
            List.of(NameTag.MIN, NameTag.MAX), variables);

        wroteTypes = !writeOutputTypes();
    }

    /**
     * Map lines from file
     * @param key       Key; line number
     * @param value     Text for specified line in file
     * @param context   Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (!skipHeader(key)) {
            if (skipComment(value)) {
                // verify parameters specified in input file
                ICsvMapperCfg cfg = getMapperCfg();
                Pair<String, String> hKeyVal = HADOOP_KEY_VAL.destringifyElement(value.toString());

                cfg.verifyTags(context.getConfiguration(), cfg, hKeyVal.getRight(), ITagger.VerifyTags.DATE);

                return;
            }

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
            Pair<String, String> hKeyVal = HADOOP_KEY_VAL.destringifyElement(value.toString());
            Pair<Boolean, LocalDate> filterRes = getDateAndFilter(hKeyVal.getLeft());
            if (filterRes.getLeft()) {
                outputList.clear();
                valuesOut.clear();

                Map<String, String> map = MAP_STRINGIFIER.mapify(hKeyVal.getRight());

                // load the result with current values

                DateWritable outKey = DateWritable.ofDate(hKeyVal.getLeft(), getKeyOutDateTimeFormatter());

                map.forEach((key1, value1) ->
                    outputTypes.entrySet().stream()
                        .filter(es -> es.getKey().equals(key1))
                        .findFirst()
                        .ifPresent(es -> {
                            String name = es.getKey();
                            Class<?> cls = es.getValue().getCls();

                            valuesOut.put(name, Value.of(value1, cls, getDateTimeFormatter(), getLogger()));
                        }));

                // read and normalise the variables
                variables.forEach(var -> {
                    OpTypeEntry typeEntry = outputTypes.get(var);

                    if (valuesOut.containsKey(var)) {

                        if (Number.class.isAssignableFrom(typeEntry.getCls())) {
                            // its a number so normalise
                            double min = stats.getOrDefault(NameTag.MIN.getKeyTag(var), 0.0);
                            double max = stats.getOrDefault(NameTag.MAX.getKeyTag(var), 0.0);
                            if (min != max) {
                                // normalise and update output type info
                                double normalised = (valuesOut.get(var).doubleValue() - min) / (max - min);
                                valuesOut.put(var, Value.of(normalised));

                                outputTypes.put(var, OpTypeEntry.of(Double.class, typeEntry.getSrc()));
                            }
                        }
                    } else if (addToOutput(typeEntry)){
                        valuesOut.put(var, Value.of(0.0));

                        outputTypes.put(var, OpTypeEntry.of(Double.class, typeEntry.getSrc()));
                    }
                });

                counter.increment();

                outputList.put(outKey, valuesOut);

                if (!wroteTypes) {
                    RegressionWritable<String, Value> types = new RegressionWritable<>();

                    // set class names in 'types'
                    outputTypes.forEach((key1, value1) -> {
                        types.put(key1, Value.of(
                            COMMA.stringifyElement(value1.getCls().getSimpleName(), value1.getSrc()))
                        );
                    });

                    outputList.put(DateWritable.MIN, types);

                    wroteTypes = true;
                }


                outputList.forEach((dwKey, val) -> {
                    try {
                        write(context, dwKey, val);
                    } catch (IOException | InterruptedException e) {
                        getLogger().warn("Exception writing mapper output", e);
                    }
                });
            }
        }
    }

    protected boolean writeOutputTypes() {
        return true;
    }

    protected boolean addToOutput(OpTypeEntry typeEntry) {
        return true;
    }

    /**
     * Read the values from the stats output
     * @param statsPath
     * @param conf
     * @param prefixes
     * @return
     */
    protected Map<String, Double> readStats(String statsPath, Configuration conf, List<NameTag> tags, List<String> prefixes) {
        FileReader fileReader = new FileReader(statsPath, conf);
        Map<String, Double> stats = new HashMap<>();

        List<String> tagRegex = tags.stream()
            .map(t -> t.getKeyTag("\\w+") + ".*")
            .collect(Collectors.toList());

        try {
            List<String> allStats = fileReader.open()
                .getAllLines(l -> tagRegex.stream().anyMatch(l::matches));
            allStats.stream()
                .filter(l -> prefixes.stream().anyMatch(Objects.requireNonNull(l)::startsWith))
                .forEach(l -> {
                    Pair<String, String> keyVal = HADOOP_KEY_VAL.destringifyElement(l);
                    stats.put(keyVal.getLeft(), Double.parseDouble(keyVal.getRight()));
                });
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            fileReader.close();
        }
        return stats;
    }


    // mapper config
    private static final ICsvMapperCfg sCfgChk = new AbstractCsvMapperCfg(NORMALISE_PROP_SECTION) {

        private final Property cswInPath = Property.of(CSW_IN_PATH_PROP, "path to CSW input file", "");
        private final Property csInPath = Property.of(CS_IN_PATH_PROP, "path to CS input file", "");
        private final Property cwInPath = Property.of(CW_IN_PATH_PROP, "path to CW input file", "");

        @Override
        public List<Property> getAdditionalProps() {
            List<Property> properties = new ArrayList<>(getPropertyList(
                List.of(OUTPUTTYPES_PATH_PROP, VARIABLES_PROP, STATS_INPUT_PATH_PROP, FACTOR_PROP)));
            properties.addAll(List.of(cswInPath, csInPath, cwInPath));
            return properties;
        }

        @Override
        public List<Property> getRequiredProps() {
            List<Property> list = new ArrayList<>(super.getRequiredProps());
            list.addAll(getAdditionalProps());
            return list;
        }

        @Override
        public List<String> getPropertyIndices() {
            return List.of();
        }
    };

    @Override
    public ICsvMapperCfg getMapperCfg() {
        return getClsCsvMapperCfg();
    }

    public static ICsvMapperCfg getClsCsvMapperCfg() {
        return sCfgChk;
    }


    public static final String PARTITION = "partition";

    public static abstract class AbstractCwsNormaliseMapper extends NormaliseMapper {

        private final int partition;

        public AbstractCwsNormaliseMapper(String section) {
            partition = MERGE_SECTIONS.indexOf(section);
        }

        @Override
        public void write(Context context, DateWritable key, RegressionWritable<String, Value> value) throws IOException, InterruptedException {
            // add destination partition to output value
            value.put(PARTITION, Value.of(partition));
            super.write(context, key, value);
        }
    }

    public static class CwsNormaliseMapper extends AbstractCwsNormaliseMapper {

        public CwsNormaliseMapper() {
            super(CRIME_WEATHER_STOCK);
        }
    }

    public static class CwNormaliseMapper extends AbstractCwsNormaliseMapper {

        public CwNormaliseMapper() {
            super(CRIME_WEATHER);
        }

        @Override
        protected boolean writeOutputTypes() {
            return false;
        }

        @Override
        protected boolean addToOutput(OpTypeEntry typeEntry) {
            // everything except stock
            return !typeEntry.getSrc().equals(STOCK_PROP_SECTION);
        }
    }

    public static class CsNormaliseMapper extends AbstractCwsNormaliseMapper {

        public CsNormaliseMapper() {
            super(CRIME_STOCK);
        }

        @Override
        protected boolean writeOutputTypes() {
            return false;
        }

        @Override
        protected boolean addToOutput(OpTypeEntry typeEntry) {
            // everything except weather
            return !typeEntry.getSrc().equals(WEATHER_PROP_SECTION);
        }
    }

}

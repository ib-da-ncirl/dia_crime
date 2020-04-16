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

import ie.ibuttimer.dia_crime.hadoop.AbstractCsvMapper;
import ie.ibuttimer.dia_crime.hadoop.ICsvMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.crime.IOutputType;
import ie.ibuttimer.dia_crime.hadoop.merge.IDecorator;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.regression.RegressionWritable;
import ie.ibuttimer.dia_crime.misc.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.ElementStringify.HADOOP_KEY_VAL;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.MAP_STRINGIFIER;

/**
 * Statistics mapper that outputs property value, property value squared and, property product values
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : property name plus specific identifier for squared value etc.
 * - output value : value
 */
public class StatsMapper extends AbstractCsvMapper<Text, Value> {

    private Counters.MapperCounter counter;

    private Map<String, IOutputType.OpTypeEntry> outputTypes;

    private List<String> variables;

    private RegressionWritable<String, Value> valuesOut = new RegressionWritable<>();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        super.initIndices(context, sCfgChk.getPropertyIndices());

        counter = getCounter(context, CountersEnum.STATS_MAPPER_COUNT);

        setLogger(getClass());

        Configuration conf = context.getConfiguration();
        StatsConfigReader cfgReader = new StatsConfigReader(getMapperCfg());

        variables = cfgReader.readVariables(conf);
        outputTypes = cfgReader.readOutputTypes(conf);

        if (show(DebugLevel.VERBOSE)) {
            setDecorator(new IDecorator<>() {
                @Override
                public Object transformKey(Text key) {
                    return String.format("%-15s", key.toString());
                }

                @Override
                public Object transformValue(Value value) {
                    AtomicReference<String> str = new AtomicReference<>("");
                    value.ifPresent(v-> str.set(v.toString()));
                    return str.get();
                }
            }, IDecorator.DecorMode.TRANSFORM);
        }
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

                cfg.verifyTags(context.getConfiguration(), cfg, hKeyVal.getRight());

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
                Map<String, String> map = MAP_STRINGIFIER.mapify(hKeyVal.getRight());
                List<String> skipList = new ArrayList<>();

                // collect the value and squared value for each property
                outputTypes.forEach((name, typeEntry) -> {
                    String readValue = map.getOrDefault(name, Value.getDefaultValueStr(typeEntry.getCls()));

                    Value wrapped = Value.of(readValue, typeEntry.getCls(), getDateTimeFormatter(), getLogger());
                    Value squared = wrapped.copyOf();
                    squared.pow(2);

                    valuesOut.put(name, wrapped);
                    valuesOut.put(NameTag.SQ.getKeyTag(name), squared);

                    // collect the product value with each other property
                    outputTypes.entrySet().stream()
                        .filter(es -> !es.getKey().equals(name))    // not same property
                        .filter(es ->
                            // reversed properties are not in skip list
                            skipList.stream()
                                .noneMatch(NameTag.getKeyPair(name, es.getKey())::equals)
                        )
                        .forEach(es -> {
                            String propName = es.getKey();
                            String leftRight = NameTag.getKeyPair(name, propName);

                            // no need to calc right-left as its the same as left-right
                            skipList.add(NameTag.getKeyPair(propName, name));

                            String readPropValue = map.getOrDefault(propName, "");

                            Value wrappedProduct = Value.of(readPropValue, es.getValue().getCls(), getDateTimeFormatter());
                            wrappedProduct.multiply(wrapped);

                            valuesOut.put(NameTag.PRD.getKeyTag(leftRight), wrappedProduct);
                        });

                });

                counter.increment();

                /* output following key/values:
                    <property name> - value
                    <property name>-SQ - squared value
                    <property name1>+<property name2>-PRD - product of 2 properties value
                 */
                valuesOut.forEach((name, val) -> {
                    try {
                        write(context, new Text(name), val);
                    } catch (IOException | InterruptedException e) {
                        getLogger().warn("Exception writing mapper output", e);
                    }
                });
            }
        }
    }

    // mapper config
    private static final ICsvMapperCfg sCfgChk = new AbstractCsvMapperCfg(STATS_PROP_SECTION) {

        @Override
        public List<Property> getAdditionalProps() {
            List<Property> list = getPropertyList(List.of(OUTPUTTYPES_PATH_PROP, VARIABLES_PROP, FACTOR_PROP,
                DEPENDENT_PROP));
            list.add(Property.of(STATS_PATH_PROP, "path for stats output", ""));
            return list;
        }

        @Override
        public List<Property> getRequiredProps() {
            List<Property> list = new ArrayList<>(super.getRequiredProps());
            list.addAll(getPropertyList(List.of(OUTPUTTYPES_PATH_PROP, VARIABLES_PROP)));
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
}

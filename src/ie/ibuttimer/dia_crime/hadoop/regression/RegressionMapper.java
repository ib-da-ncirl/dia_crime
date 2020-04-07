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
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.ICsvEntryMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.stats.StatsConfigReader;
import ie.ibuttimer.dia_crime.misc.ConfigReader;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import ie.ibuttimer.dia_crime.misc.PropertyWrangler;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

public class RegressionMapper extends AbstractCsvMapper<Text, RegressionWritable<?, ?>> {

    private Counters.MapperCounter counter;

    private MapStringifier.ElementStringify hadoopKeyVal = new MapStringifier.ElementStringify("\t");
    private MapStringifier.ElementStringify commaStringify = new MapStringifier.ElementStringify(",");

    private Map<String, Class<?>> outputTypes;

    private List<String> independents;
    private String dependent;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        super.setup(context, sCfgChk.getPropertyIndices());

        counter = getCounter(context, CountersEnum.REGRESSION_MAPPER_COUNT);

        setLogger(getClass());

        Configuration conf = context.getConfiguration();
        StatsConfigReader cfgReader = new StatsConfigReader(getEntryMapperCfg());

        outputTypes = cfgReader.readOutputTypes(conf);
        independents = cfgReader.readCommaSeparatedProperty(conf, INDEPENDENTS_PROP);
        dependent = getConfigProperty(conf, DEPENDENT_PROP);
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

        if (!skip(key, value)) {
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
            Pair<String, String> hKeyVal = hadoopKeyVal.destringifyElement(value.toString());
            Pair<Boolean, LocalDate> filterRes = getDateAndFilter(hKeyVal.getLeft());
            if (filterRes.getLeft()) {
                Map<String, String> map = MapStringifier.mapify(hKeyVal.getRight());

                throw new UnsupportedOperationException("fix regression");
//                RegressionWritable<String, Object> entry = new RegressionWritable<>();
//
//                outputTypes.forEach((name, cls) -> {
//
//                    boolean required = independents.stream().anyMatch(name::equals);
//                    if (required) {
//                        String readValue = map.getOrDefault(name, "");
//
//                        Value wrapped = Value.of(readValue, cls, getDateTimeFormatter());
//
//                        wrapped.addTo(entry, name);
//                    }
//                });
//
//                counter.increment();
//
//                write(context, new Text(key.toString()), entry);
            }
        }
    }



    private static ICsvEntryMapperCfg sCfgChk = new AbstractCsvEntryMapperCfg(REGRESSION_PROP_SECTION) {

        private Property typesPathProp = Property.of(OUTPUTTYPES_PATH_PROP, "path to output types file", "");
        private Property indoProp = Property.of(INDEPENDENTS_PROP, "list of independent variables to use", "");
        private Property depProp = Property.of(DEPENDENT_PROP, "dependent variable to use", "");

        @Override
        public List<Property> getAdditionalProps() {
            return List.of(typesPathProp, indoProp, depProp);
        }

        @Override
        public List<Property> getRequiredProps() {
            List<Property> list = super.getRequiredProps();
            list.add(typesPathProp);
            list.add(indoProp);
            list.add(depProp);
            return list;
        }

        @Override
        public List<String> getPropertyIndices() {
            return List.of();
        }
    };

    @Override
    public ICsvEntryMapperCfg getEntryMapperCfg() {
        return getCsvEntryMapperCfg();
    }

    public static ICsvEntryMapperCfg getCsvEntryMapperCfg() {
        return sCfgChk;
    }
}

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

import com.google.common.base.Charsets;
import ie.ibuttimer.dia_crime.hadoop.AbstractCsvMapper;
import ie.ibuttimer.dia_crime.hadoop.ICsvEntryMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.io.FileUtil;
import ie.ibuttimer.dia_crime.hadoop.misc.CounterEnums;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import ie.ibuttimer.dia_crime.misc.PropertyWrangler;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.http.util.TextUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

public class RegressionMapper extends AbstractCsvMapper<Text, RegressionWritable<?, ?>> {

    private static List<Class<?>> CLASSES = List.of(Integer.class, Long.class, Float.class, Double.class, String.class,
        LocalDate.class);


    private CounterEnums.MapperCounter counter;

    private MapStringifier.ElementStringify hadoopKeyVal = new MapStringifier.ElementStringify("\t");
    private MapStringifier.ElementStringify commaStringify = new MapStringifier.ElementStringify(",");

    private Map<String, Class<?>> outputTypes;

    private List<String> independents;
    private String dependent;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        super.setup(context, sCfgChk.getPropertyIndices());

        counter = getCounter(context, RegressionCountersEnum.MAPPER_COUNT);

        setLogger(getClass());

        Configuration conf = context.getConfiguration();
        outputTypes = readOutputTypeClasses(conf);
        independents = readIndependents(conf);
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

        if (!skipHeader(key)) {
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
                RegressionWritable<String, Object> entry = new RegressionWritable<>();

                outputTypes.forEach((name, cls) -> {

                    boolean required = independents.stream().anyMatch(name::equals);
                    if (required) {
                        String readValue = map.getOrDefault(name, "");

                        Value wrapped = Value.of(readValue, cls, getDateTimeFormatter());

                        wrapped.addTo(entry, name);
                    }
                });

                counter.increment();

                write(context, new Text(key.toString()), entry);
            }
        }
    }


    private Map<String, Class<?>> readOutputTypeClasses(Configuration conf) {
        Map<String, Class<?>> outputTypes = new HashMap<>();
        String typesPath = getConfigProperty(conf, OUTPUTTYPES_PATH_PROP);

        Map<String, String> entries = new HashMap<>();
        FileUtil fileUtil = new FileUtil(new Path(typesPath), conf);

        try (FSDataInputStream stream = fileUtil.fileReadOpen();
             InputStreamReader inputStream = new InputStreamReader(stream, Charsets.UTF_8);
             BufferedReader reader = new BufferedReader(inputStream)) {

            reader.lines()
                .map(commaStringify::destringifyElement)
                .filter(p -> !TextUtils.isEmpty(p.getLeft()) && !TextUtils.isEmpty(p.getRight()))
                .forEach(p -> entries.put(p.getLeft(), p.getRight()));

        } catch (IOException e) {
            e.printStackTrace();
        }

        entries.forEach((key, name) -> {
            CLASSES.stream()
                .filter(cls -> name.equals(cls.getSimpleName()))
                .findFirst()
                .ifPresent(cls -> outputTypes.put(key, cls));
        });
        if (outputTypes.size() != entries.size()) {
            throw new IllegalStateException("Unmatched output type class");
        }
        return outputTypes;
    }

    private List<String> readIndependents(Configuration conf) {
        String indos = getConfigProperty(conf, INDEPENDENTS_PROP);
        List<String> independents = Arrays.stream(indos.split(","))
            .map(String::trim)
            .collect(Collectors.toList());

        if (independents.size() == 0) {
            throw new IllegalStateException("No independents specified");
        }
        return independents;
    }



    protected CounterEnums.MapperCounter getCounter(Context context, RegressionCountersEnum countersEnum) {
        return new CounterEnums.MapperCounter(context, countersEnum.getClass().getName(), countersEnum.toString());
    }

    private static ICsvEntryMapperCfg sCfgChk = new AbstractCsvEntryMapperCfg() {

        private PropertyWrangler propertyWrangler = new PropertyWrangler(REGRESSION_PROP_SECTION);

        private Property indoPathProp = Property.of(OUTPUTTYPES_PATH_PROP, "path to output types file", "");
        private Property indoProp = Property.of(INDEPENDENTS_PROP, "list of independent variables to use", "");
        private Property depProp = Property.of(DEPENDENT_PROP, "dependent variable to use", "");

        @Override
        public List<Property> getAdditionalProps() {
            return List.of(indoPathProp, indoProp, depProp);
        }

        @Override
        public List<Property> getRequiredProps() {
            List<Property> list = super.getRequiredProps();
            list.add(indoPathProp);
            list.add(indoProp);
            list.add(depProp);
            return list;
        }

        @Override
        public String getPropertyRoot() {
            return REGRESSION_PROP_SECTION;
        }

        @Override
        public String getPropertyPath(String propertyName) {
            return propertyWrangler.getPropertyPath(propertyName);
        }

        @Override
        public List<String> getPropertyIndices() {
            return List.of();
        }
    };

    @Override
    protected ICsvEntryMapperCfg getEntryMapperCfg() {
        return getCsvEntryMapperCfg();
    }

    public static ICsvEntryMapperCfg getCsvEntryMapperCfg() {
        return sCfgChk;
    }
}

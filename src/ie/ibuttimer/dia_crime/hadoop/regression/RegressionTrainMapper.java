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
import ie.ibuttimer.dia_crime.hadoop.ICsvMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.stats.NameTag;
import ie.ibuttimer.dia_crime.hadoop.stats.StatsConfigReader;
import ie.ibuttimer.dia_crime.misc.ConfigReader;
import ie.ibuttimer.dia_crime.misc.DebugLevel;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Regression mapper for model generation
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : the current epoch number
 * - output value : RegressionWritable containing individual values
 */
public class RegressionTrainMapper extends AbstractRegressionMapper<Text, String, Value> {

    private Counters.MapperCounter counter;

    private String epoch;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        counter = getCounter(context, CountersEnum.REGRESSION_MAPPER_COUNT);

        Configuration conf = context.getConfiguration();
        StatsConfigReader cfgReader = new StatsConfigReader(getMapperCfg());

        epoch = cfgReader.getConfigProperty(conf, CURRENT_EPOCH_PROP, "1");
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

        FilterResult filterRes = filterDate(key, value, context);
        if (filterRes.pass) {
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
            // collect the value for each property
            RegressionWritable<String, Value> entry = collectValues(filterRes.value);

            double yi = entry.getProperty(dependent).doubleValue();

            Map<String, Double> xi = new HashMap<>();

            independents.forEach(indo -> {
                xi.put(indo, entry.getProperty(indo).doubleValue());
            });

            double ei = regressor.error(yi, xi);
            double se = regressor.sqError(ei);
            Map<String, Double> pdw = regressor.partialDerivativeWeight(xi, ei);
            double pdb = regressor.partialDerivativeBias(ei);

            pdw.forEach((indo, val) -> {
                entry.put(NameTag.PDW.getKeyTag(indo), Value.of(val));
            });
            entry.put(NameTag.ERR.getKeyTag(dependent), Value.of(ei));
            entry.put(NameTag.PDB.getKeyTag(dependent), Value.of(pdb));
            independents.forEach(indo -> {

                entry.put(NameTag.getKeyTagChain(indo, List.of(NameTag.ERR, NameTag.SQ)), Value.of(se));

                entry.put(NameTag.CNT.getKeyTag(indo), Value.of(counts.get(indo)));

            });
            if (show(DebugLevel.HIGH)) {
                // 1 yi=1.0,06=1.0,06-CNT=59.0,06-ERR-SQ=1.0,06-PDW=-2.0,total=1.0,total-ERR=1.0,total-PDB=-2.0
                StringBuffer sb = new StringBuffer()
                    .append("yi=").append(yi);
                entry.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(es -> {
                        if (sb.length() > 0) {
                            sb.append(',');
                        }
                        sb.append(es.getKey()).append('=').append(es.getValue().doubleValue());
                    });
                getLogger().info(epoch + " " + sb.toString());
            }

            try {
                context.write(new Text(epoch), entry);

                counter.increment();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // configuration object
    private static final ICsvMapperCfg sCfgChk = new RegressionMapperCfg(REGRESSION_PROP_SECTION) {

        private final Property startProp = Property.of(TRAIN_START_DATE_PROP, "training start date", "");
        private final Property endProp = Property.of(TRAIN_END_DATE_PROP, "training end date", "");
        private final Property epochLimitProp = Property.of(EPOCH_LIMIT_PROP, "maximum number of epoch", "");
        private final Property currentEpochProp = Property.of(CURRENT_EPOCH_PROP, "current epoch", "");
        private final Property targetCostProp = Property.of(TARGET_COST_PROP, "target cost for regression", "");
        private final Property steadyTargetProp = Property.of(STEADY_TARGET_PROP, "steady target", "");
        private final Property steadyLimitCostProp = Property.of(STEADY_LIMIT_PROP, "steady limit", "");
        private final Property timeLimitCostProp = Property.of(TARGET_TIME_PROP, "max duration in minutes", "");

        private final List<Property> required = List.of(startProp, endProp);
        private final List<Property> notRequired = List.of(epochLimitProp, currentEpochProp, targetCostProp,
                                                            steadyTargetProp, steadyLimitCostProp, timeLimitCostProp);

        @Override
        public List<Property> getAdditionalProps() {
            List<Property> properties = new ArrayList<>(super.getAdditionalProps());
            properties.addAll(required);
            properties.addAll(notRequired);
            return properties;
        }

        @Override
        public List<Property> getRequiredProps() {
            List<Property> list = new ArrayList<>(super.getRequiredProps());
            list.addAll(required);
            return list;
        }

        @Override
        public Pair<Integer, List<String>> checkConfiguration(Configuration conf) {
            Pair<Integer, List<String>> chkRes = super.checkConfiguration(conf);
            int resultCode = chkRes.getLeft();
            List<String> errors = new ArrayList<>(chkRes.getRight());

            // check training dates
            Pair<Integer, List<String>> dateRes = checkDatePairConfiguration(
                conf, TRAIN_START_DATE_PROP, TRAIN_END_DATE_PROP);
            if (dateRes.getLeft() != ECODE_SUCCESS) {
                errors.addAll(dateRes.getRight());
                resultCode = dateRes.getLeft();
            }
            dateRes = checkDatePairConfiguration(
                conf, FILTER_START_DATE_PROP, TRAIN_START_DATE_PROP);
            if (dateRes.getLeft() != ECODE_SUCCESS) {
                errors.addAll(dateRes.getRight());
                resultCode = dateRes.getLeft();
            }
            dateRes = checkDatePairConfiguration(
                conf, TRAIN_END_DATE_PROP, FILTER_END_DATE_PROP);
            if (dateRes.getLeft() != ECODE_SUCCESS) {
                errors.addAll(dateRes.getRight());
                resultCode = dateRes.getLeft();
            }

            ConfigReader cfgReader = new ConfigReader(this);
            long maxEpochs = cfgReader.getConfigProperty(conf, EPOCH_LIMIT_PROP, 0L).longValue();
            double targetCost = cfgReader.getConfigProperty(conf, TARGET_COST_PROP, 0.0).doubleValue();
            double steadyTarget = cfgReader.getConfigProperty(conf, STEADY_TARGET_PROP, 0.0).doubleValue();
            long steadyLimit = cfgReader.getConfigProperty(conf, STEADY_LIMIT_PROP, 0).intValue();

            if (maxEpochs <= 0 && targetCost <= 0 && steadyLimit <= 0) {
                errors.add("Error: No regression termination condition specified");
                resultCode = ECODE_CONFIG_ERROR;
            }
            if (steadyLimit > 0 && steadyTarget <= 0) {
                errors.add("Error: Invalid steady target number of decimal places condition specified");
                resultCode = ECODE_CONFIG_ERROR;
            }

            return Pair.of(resultCode, errors);
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

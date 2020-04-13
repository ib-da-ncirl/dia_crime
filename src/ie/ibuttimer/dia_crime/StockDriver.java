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

package ie.ibuttimer.dia_crime;

import ie.ibuttimer.dia_crime.hadoop.stock.DowJonesStockMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.NasdaqStockMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.SP500StockMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.StockReducer;
import ie.ibuttimer.dia_crime.misc.Constants;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.util.*;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Hadoop driver class for stock related jobs
 */
public class StockDriver extends AbstractDriver {

    private static final Logger logger = Logger.getLogger(StockDriver.class);

    public static List<String> STOCK_SECTIONS =
        Arrays.asList(NASDAQ_PROP_SECTION, DOWJONES_PROP_SECTION, SP500_PROP_SECTION);
    public static List<String> STOCK_IDS =
        Arrays.asList(NASDAQ_ID, DOWJONES_ID, SP500_ID);

    public StockDriver(DiaCrimeMain app) {
        super(app);
    }

    public static StockDriver of(DiaCrimeMain app) {
        return new StockDriver(app);
    }

    public static Pair<List<String>, List<String>> getSectionLists() {
        // List<String> sections, List<String> commonSection
        return Pair.of(STOCK_SECTIONS, Collections.singletonList(STOCK_PROP_SECTION));
    }

    public static void addStockSpecificsToConfig(Configuration conf) {
        StringBuilder sb = new StringBuilder();
        STOCK_IDS.stream()
            .map(s -> s + ",")
            .forEach(sb::append);
        conf.set(generatePropertyName(STOCK_PROP_SECTION, ID_LIST_PROP), sb.toString());
    }

    public Job getStockJob(Properties properties) throws Exception {

        Pair<List<String>, List<String>> sectionLists = getSectionLists();

        Job job = null;
        Configuration conf = new Configuration();
        int resultCode = readConfigs(conf, properties, sectionLists.getLeft(), sectionLists.getRight());

        if (resultCode == Constants.ECODE_SUCCESS) {
            Map<String, SectionCfg> sections = new HashMap<>();

            addStockSpecificsToConfig(conf);

            sections.put(NASDAQ_PROP_SECTION, SectionCfg.of(NasdaqStockMapper.class));
            sections.put(DOWJONES_PROP_SECTION, SectionCfg.of(DowJonesStockMapper.class));
            sections.put(SP500_PROP_SECTION, SectionCfg.of(SP500StockMapper.class));

            job = initJob("Stocks", conf, sections);

            job.setReducerClass(StockReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(MapWritable.class);

            /*
             * Input and Output types of a MapReduce job:
             * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
             * (input) <LongWritable, Text> -> map -> <Text, MapWritable> -> reduce -> <Text, Text> (output)
             */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }

        return job;
    }

    public int runStockJob(JobConfig cfg) throws Exception {

        int resultCode = Constants.ECODE_FAIL;
        Job job = getStockJob(cfg.properties);
        if (job != null) {
            if (cfg.wait) {
                resultCode = job.waitForCompletion(cfg.verbose) ? Constants.ECODE_SUCCESS : Constants.ECODE_FAIL;
            } else {
                job.submit();
                resultCode = ECODE_RUNNING;
            }
        }

        return resultCode;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}

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

package ie.ibuttimer.dia_crime.hadoop.crime;

import ie.ibuttimer.dia_crime.hadoop.io.FileUtil;
import ie.ibuttimer.dia_crime.hadoop.misc.CounterEnums;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import ie.ibuttimer.dia_crime.misc.PropertyWrangler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.Utils.iterableOfMapsToList;

public class CrimeReducer extends AbstractCrimeReducer<Text, MapWritable, Text, Text> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        setLogger(getClass());
    }

    /**
     * Reduce the values for a key
     * @param key       Key value; date string
     * @param values    Values for the specified key
     * @param context   Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

        CounterEnums.ReducerCounter counter = getCounter(context, CrimeCountersEnum.REDUCER_COUNT);

        // flatten the maps in values and get a list of the CrimeEntryWritables
        List<CrimeWritable> crimes = iterableOfMapsToList(values, CrimeWritable.class);

        Map<String, Integer> map = reduceToTotalsPerCategory(crimes, counter, this);

        // create value string of <category>:<count> separated by ',' with <total>:<count> at the end
        // e.g. 2001-01-01	01A:2, 02:87, 03:41, 04A:28, 04B:44, 05:66, 06:413, 07:60, 08A:43, 08B:252, 10:12, 11:73, 12:7, 14:233, 15:32, 16:5, 17:68, 18:89, 19:2, 20:44, 22:3, 24:4, 26:211, total:1819
        write(context, key, new Text(MapStringifier.stringify(map)));
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        outputCategories(context,this, getLogger());
    }

    public static Map<String, Integer> reduceToTotalsPerCategory(
        List<CrimeWritable> values, CounterEnums.ReducerCounter counter, ICrimeReducer reducer) {

        Map<String, Integer> counts = new HashMap<>();

        // get counts for each category
        values.forEach(value -> {
            String category = value.getFbiCode();

            if (!counts.containsKey(category)) {
                counts.put(category, 0);

                reducer.addCategory(category);
            }
            counts.put(category, counts.get(category) + 1);

            if (counter != null) {
                counter.incrementValue(1);
            }
        });

        // sort based on category
        int total = 0;  // total crimes for the day
        Map<String, Integer> map = new TreeMap<>();
        for (String category : counts.keySet()) {
            int count = counts.get(category);
            total += count;
            map.put(category, count);
        }
        map.put(TOTAL_PROP, total);

        return map;
    }


    public static void outputCategories(Reducer<?,?,?,?>.Context context, ICrimeReducer reducer, Logger logger) {
        // TODO check this in case of multiple reducers
        if (context.getProgress() == 1.0) {
            Configuration conf = context.getConfiguration();
            PropertyWrangler propertyWrangler = new PropertyWrangler(CRIME_PROP_SECTION);
            Set<String> categorySet = reducer.getCategorySet();

            Path outDir = new Path(conf.get(propertyWrangler.getPropertyPath(OUT_PATH_PROP)));
            String outPath = conf.get(CATEGORIES_PATH_PROP, "categories.txt");
            FileUtil fileUtil = new FileUtil(outDir, conf);

            try (FSDataOutputStream stream = fileUtil.fileWriteOpen(outPath, true)) {
                StringBuffer sb = new StringBuffer();
                categorySet.stream()
                    .map(s -> s + ", ")
                    .forEach(sb::append);
                fileUtil.write(stream, sb.toString());
            } catch (IOException ioe) {
                logger.warn("Unable to output categories file", ioe);
            } finally {
                try {
                    fileUtil.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            logger.info("Skipping output of categories file, job incomplete: " + context.getProgress());
        }
    }
}


// attempt at combiner version but leave for now
//public class CrimeReducer extends Reducer<Text, SortedMapWritable<Text>, Text, Text> {
//
//    /**
//     * Reduce the values for a key
//     * @param key       Key value; date string
//     * @param values    Values for the specified key
//     * @param context   Current context
//     * @throws IOException
//     * @throws InterruptedException
//     */
//    @Override
//    protected void reduce(Text key, Iterable<SortedMapWritable<Text>> values, Context context) throws IOException, InterruptedException {
//
//        // create value string of <category>:<count> separated by ',' with <total>:<count> at the end
//        // e.g. 2001-01-01	01A:2, 02:87, 03:41, 04A:28, 04B:44, 05:66, 06:413, 07:60, 08A:43, 08B:252, 10:12, 11:73, 12:7, 14:233, 15:32, 16:5, 17:68, 18:89, 19:2, 20:44, 22:3, 24:4, 26:211, total:1819
//        StringBuffer sb = new StringBuffer();
//        for (SortedMapWritable<Text> category : values) {
//            for (Map.Entry<Text, Writable> entry : category.entrySet()) {
//                if (sb.length() > 0) {
//                    sb.append(", ");
//                }
//                sb.append(entry.getKey().toString())
//                        .append(":")
//                        .append(entry.getValue());
//            }
//        }
//
//        context.write(key, new Text(sb.toString()));
//    }
//}

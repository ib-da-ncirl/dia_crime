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

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class CrimeEntryReducer extends Reducer<Text, CrimeEntryWritable, Text, Text> {

    /**
     * Reduce the values for a key
     * @param key       Key value; date string
     * @param values    Values for the specified key
     * @param context   Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<CrimeEntryWritable> values, Context context) throws IOException, InterruptedException {

        Map<String, Integer> crimeCategoryCounts = new HashMap<>();

        // get counts for each category
        for (CrimeEntryWritable entry : values) {
            String category = entry.getFbiCode();

            if (!crimeCategoryCounts.containsKey(category)) {
                crimeCategoryCounts.put(category, 0);
            }
            crimeCategoryCounts.put(category, crimeCategoryCounts.get(category) + 1);
        }

        // sort based on category
        int total = 0;  // total crimes for the day
        Map<String, Integer> crimeCategoryCountMap = new TreeMap<>();
        for (String category : crimeCategoryCounts.keySet()) {
            int count = crimeCategoryCounts.get(category);
            total += count;
            crimeCategoryCountMap.put(category, count);
        }

        // create value string of <category>:<count> separated by ',' with <total>:<count> at the end
        // e.g. 2001-01-01	01A:2, 02:87, 03:41, 04A:28, 04B:44, 05:66, 06:413, 07:60, 08A:43, 08B:252, 10:12, 11:73, 12:7, 14:233, 15:32, 16:5, 17:68, 18:89, 19:2, 20:44, 22:3, 24:4, 26:211, total:1819
        StringBuilder sb = new StringBuilder();
        for (String category : crimeCategoryCountMap.keySet()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(category)
                    .append(":")
                    .append(crimeCategoryCounts.get(category));
        }
        sb.append(", total:")
            .append(total);

        context.write(key, new Text(sb.toString()));
    }
}


// attempt at combiner version but leave for now
//public class CrimeEntryReducer extends Reducer<Text, SortedMapWritable<Text>, Text, Text> {
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

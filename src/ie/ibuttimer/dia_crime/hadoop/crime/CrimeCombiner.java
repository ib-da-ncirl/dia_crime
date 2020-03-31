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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CrimeCombiner extends Reducer<Text, CrimeWritable, Text, SortedMapWritable<Text>> {

    /**
     * Reduce the values for a key
     * @param key       Key value; date string
     * @param values    Values for the specified key
     * @param context   Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<CrimeWritable> values, Context context) throws IOException, InterruptedException {

        Map<String, Integer> crimeCategoryCounts = new HashMap<>();

        // get counts for each category
        for (CrimeWritable entry : values) {
            String category = entry.getFbiCode();

            int count = crimeCategoryCounts.getOrDefault(category, 0);
            crimeCategoryCounts.put(category, count + 1);
        }

        // sort based on category
        int total = 0;  // total crimes for the day
        SortedMapWritable<Text> crimeCategoryCountTree = new SortedMapWritable<>();
        for (String category : crimeCategoryCounts.keySet()) {
            int count = crimeCategoryCounts.get(category);
            total += count;
            crimeCategoryCountTree.put(new Text(category), new IntWritable(count));
        }
        crimeCategoryCountTree.put(new Text("total"), new IntWritable(total));

        context.write(key, crimeCategoryCountTree);
    }
}

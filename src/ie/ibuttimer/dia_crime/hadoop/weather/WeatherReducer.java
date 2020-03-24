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

package ie.ibuttimer.dia_crime.hadoop.weather;

import ie.ibuttimer.dia_crime.misc.Counter;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class WeatherReducer extends Reducer<Text, MapWritable, Text, Text> {

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

        // get average for all daily measurements
        WeatherWritable avgCalc = new WeatherWritable();
        AtomicInteger total = new AtomicInteger();
        Counter<Integer, Triple<Integer, String, String>> descCounter = new Counter<>();

        values.forEach(hourlyVal -> {
            hourlyVal.forEach((hourKey, hourEntry) -> {
                WeatherWritable weather = (WeatherWritable)hourEntry;
                Triple<Integer, String, String> toCount =
                    Triple.of(weather.getWeatherId(), weather.getWeatherMain(), weather.getWeatherDescription());
                descCounter.inc(toCount.getLeft(), toCount);

                avgCalc.setLocalDate(weather.getLocalDate());
                avgCalc.add(weather);

                total.incrementAndGet();
            });
        });
        avgCalc.divide(total.get());


        /* see https://openweathermap.org/weather-conditions for conditions
            in the case of more than one highest count, take the one with the highest id, as weather conditions get
            the id increases
         */
        List<Triple<Integer, String, String>> weatherDesc = descCounter.highest();
        List<Triple<Integer, String, String>> weatherList = weatherDesc;
        if (weatherDesc.size() > 1) {
            weatherList = weatherDesc.stream()
                    .sorted(Comparator.comparing(Triple::getLeft))
                    .collect(Collectors.toList());
        }
        weatherList.stream().findFirst().ifPresent(w -> {
            avgCalc.setWeatherId(w.getLeft());
            avgCalc.setWeatherMain(w.getMiddle());
            avgCalc.setWeatherDescription(w.getRight());
        });

        // create value string of <category>:<count> separated by ',' with <total>:<count> at the end
        // e.g. 2001-01-01	01A:2, 02:87, 03:41, 04A:28, 04B:44, 05:66, 06:413, 07:60, 08A:43, 08B:252, 10:12, 11:73, 12:7, 14:233, 15:32, 16:5, 17:68, 18:89, 19:2, 20:44, 22:3, 24:4, 26:211, total:1819
        context.write(key, new Text(MapStringifier.stringify(avgCalc.toMap())));
    }
}

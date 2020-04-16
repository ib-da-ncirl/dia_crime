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

import ie.ibuttimer.dia_crime.hadoop.AbstractReducer;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.misc.DateWritable;
import ie.ibuttimer.dia_crime.misc.Counter;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static ie.ibuttimer.dia_crime.misc.Constants.WEATHER_ID_NAMED_OP;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.MAP_STRINGIFIER;
import static ie.ibuttimer.dia_crime.misc.Utils.iterableOfMapsToList;

/**
 * Reducer for weather data
 * - input key : date
 * - input value : MapWritable<date, WeatherWritable>
 * - output key : date
 * - output value : value string of <field>:<value> separated by ','
 */
public class WeatherReducer extends AbstractReducer<DateWritable, MapWritable, DateWritable, Text> {

    protected static Set<Integer> weatherIDs;

    private MultipleOutputs<DateWritable, Text> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        weatherIDs = new HashSet<>();

        mos = new MultipleOutputs<>(context);
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
    protected void reduce(DateWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

        Counters.ReducerCounter counter = getCounter(context, CountersEnum.WEATHER_REDUCER_COUNT);

        // flatten the maps in values and get a list of the CrimeEntryWritables
        List<WeatherWritable> weatherReadings = iterableOfMapsToList(values, WeatherWritable.class);

        // get average for all daily measurements
        Map<String, Object> map = reduceToAverages(weatherReadings, counter);

        // create value string of <field>:<value> separated by ','
        // e.g. 2001-01-01	date:2001-01-01, temp:-5.8541665, weather_id:804, snow_3h:0.0, weather_description:overcast clouds, rain_1h:0.0, snow_1h:0.0, clouds_all:47, rain_3h:0.0, pressure:1028, feels_like:-11.464583, temp_max:-2.777917, weather_main:Clouds, temp_min:-8.428333, wind_deg:270, humidity:71, wind_speed:3.619583
        context.write(key, new Text(MAP_STRINGIFIER.stringify(map)));
    }

    /**
     * Reduce entries to averages
     * @param values
     * @param counter
     * @return
     */
    public static Map<String, Object> reduceToAverages(List<WeatherWritable> values, Counters.ReducerCounter counter) {

        // get average for all daily measurements
        WeatherWritable avgCalc = new WeatherWritable();
        AtomicInteger total = new AtomicInteger();
        Counter<Integer, Triple<Integer, String, String>> descCounter = new Counter<>();

        if (weatherIDs == null) {
            // being called from another reducer, so just create a local variable
            weatherIDs = new HashSet<>();
        }

        values.forEach(weather -> {
            weatherIDs.add(weather.getWeatherId());

            Triple<Integer, String, String> toCount =
                Triple.of(weather.getWeatherId(), weather.getWeatherMain(), weather.getWeatherDescription());
            descCounter.inc(toCount.getLeft(), toCount);

            avgCalc.setLocalDate(weather.getLocalDate());
            avgCalc.add(weather);

            total.incrementAndGet();
            counter.increment();
        });
        avgCalc.divide(total.get());


        /* see https://openweathermap.org/weather-conditions for conditions
            in the case of more than one highest count, take the one with the highest id, as weather conditions get
            worse as the id increases
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

        return avgCalc.toMap();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        if (context.getProgress() == 1.0) {
            AtomicInteger count = new AtomicInteger(0);
            weatherIDs.stream()
                .sorted()
                .forEach(wid -> {
                    try {
                        String op = wid + "," + count.getAndIncrement();
                        write(mos, WEATHER_ID_NAMED_OP, DateWritable.COMMENT_KEY, new Text(op));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
        }

        mos.close();
    }

    @Override
    protected DateWritable newKey(String key) {
        return new DateWritable();
    }

    @Override
    protected Text newValue(String value) {
        return new Text(value);
    }
}

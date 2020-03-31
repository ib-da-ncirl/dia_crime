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

import ie.ibuttimer.dia_crime.hadoop.misc.CounterEnums;
import ie.ibuttimer.dia_crime.misc.Counter;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static ie.ibuttimer.dia_crime.misc.Utils.iterableOfMapsToList;

public class WeatherReducer extends AbstractWeatherReducer<Text, MapWritable, Text, Text> {

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

        CounterEnums.ReducerCounter counter = getCounter(context, WeatherCountersEnum.REDUCER_COUNT);

        // flatten the maps in values and get a list of the CrimeEntryWritables
        List<WeatherWritable> weatherReadings = iterableOfMapsToList(values, WeatherWritable.class);

        // get average for all daily measurements
        Map<String, Object> map = reduceToAverages(weatherReadings, counter);

        // create value string of <field>:<value> separated by ','
        // e.g. 2001-01-01	date:2001-01-01, temp:-5.8541665, weather_id:804, snow_3h:0.0, weather_description:overcast clouds, rain_1h:0.0, snow_1h:0.0, clouds_all:47, rain_3h:0.0, pressure:1028, feels_like:-11.464583, temp_max:-2.777917, weather_main:Clouds, temp_min:-8.428333, wind_deg:270, humidity:71, wind_speed:3.619583
        context.write(key, new Text(MapStringifier.stringify(map)));
    }

    public static Map<String, Object> reduceToAverages(List<WeatherWritable> values, CounterEnums.ReducerCounter counter) {

        // get average for all daily measurements
        WeatherWritable avgCalc = new WeatherWritable();
        AtomicInteger total = new AtomicInteger();
        Counter<Integer, Triple<Integer, String, String>> descCounter = new Counter<>();

        values.forEach(weather -> {
            Triple<Integer, String, String> toCount =
                Triple.of(weather.getWeatherId(), weather.getWeatherMain(), weather.getWeatherDescription());
            descCounter.inc(toCount.getLeft(), toCount);

            avgCalc.setLocalDate(weather.getLocalDate());
            avgCalc.add(weather);

            total.incrementAndGet();
            counter.incrementValue(1);
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
}

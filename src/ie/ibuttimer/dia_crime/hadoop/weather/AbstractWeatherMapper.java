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

import ie.ibuttimer.dia_crime.hadoop.AbstractCsvMapper;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.ICsvMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.misc.DateTimeWritable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Mapper for a weather entry:
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : date
 * @param <VO>  output value
 */
public abstract class AbstractWeatherMapper<VO> extends AbstractCsvMapper<DateTimeWritable, VO> {

    private WeatherWritable.WeatherWritableBuilder builder;

    private Map<String, Integer> indices;
    private int maxIndex = -1;

    public static final List<String> WEATHER_PROPERTY_INDICES = WeatherWritable.FIELDS;

    private DateTimeWritable keyOut = new DateTimeWritable();

    private Counters.MapperCounter counter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        super.initIndices(context, WEATHER_PROPERTY_INDICES);

        indices = getIndices();
        maxIndex = getMaxIndex();

        builder = WeatherWritable.getBuilder();

        counter = getCounter(context, CountersEnum.WEATHER_MAPPER_COUNT);
    }

    /**
     * Map lines from a csv file
     * @param key       Key; line number
     * @param value     Text for specified line in file
     * @param context   Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // NOTE: there are multiple entries for the same hour in some cases, seems to be different weather descriptions

        if (!skip(key, value)) {
            /* dt,dt_iso,timezone,city_name,lat,lon,temp,feels_like,temp_min,temp_max,pressure,sea_level,grnd_level,humidity,
                wind_speed,wind_deg,rain_1h,rain_3h,snow_1h,snow_3h,clouds_all,weather_id,weather_main,weather_description,weather_icon
             */
            String line = value.toString();
            String[] splits = line.split(getSeparator());
            // if the line ends with separators, they are ignored and consequently the num of splits doesn't match
            // num of columns in csv file
            if (splits.length > maxIndex) {
                Pair<Boolean, LocalDateTime> filterRes = getZonedDateTimeAndFilter(splits[indices.get(DATE_PROP)]);

                if (filterRes.getLeft()) {
                    LocalDateTime dateTime = filterRes.getRight();

                    WeatherWritable entry = builder.clear()
                        .setLocalDateTime(dateTime)
                        .setTemp(splits[indices.get(TEMP_PROP)])
                        .setFeelsLike(splits[indices.get(FEELS_LIKE_PROP)])
                        .setTempMin(splits[indices.get(TEMP_MIN_PROP)])
                        .setTempMax(splits[indices.get(TEMP_MAX_PROP)])
                        .setPressure(splits[indices.get(PRESSURE_PROP)])
                        .setHumidity(splits[indices.get(HUMIDITY_PROP)])
                        .setWindSpeed(splits[indices.get(WIND_SPEED_PROP)])
                        .setWindDeg(splits[indices.get(WIND_DEG_PROP)])
                        .setRain1h(splits[indices.get(RAIN_1H_PROP)])
                        .setRain3h(splits[indices.get(RAIN_3H_PROP)])
                        .setSnow1h(splits[indices.get(SNOW_1H_PROP)])
                        .setSnow3h(splits[indices.get(SNOW_3H_PROP)])
                        .setClouds(splits[indices.get(CLOUDS_ALL_PROP)])
                        .setWeatherId(splits[indices.get(WEATHER_ID_PROP)])
                        .setWeatherMain(splits[indices.get(WEATHER_MAIN_PROP)])
                        .setWeatherDescription(splits[indices.get(WEATHER_DESC_PROP)])
                        .build();

                    counter.increment();

                    // file contains hourly entries, but just use date as the key
                    keyOut.setLocalDateTime(dateTime);

                    // return the day as the key and the crime entry as the value
                    writeOutput(context, keyOut, entry);
                }
            } else {
                getLogger().warn("Line " + key.get() + " ignored, insufficient columns: " + splits.length);
            }
        }
    }

    protected abstract void writeOutput(Context context, DateTimeWritable key, WeatherWritable value) throws IOException, InterruptedException;

    private static ICsvMapperCfg sCfgChk = new AbstractCsvMapperCfg(WEATHER_PROP_SECTION) {
        @Override
        public List<String> getPropertyIndices() {
            return WEATHER_PROPERTY_INDICES;
        }
    };

    @Override
    public ICsvMapperCfg getEntryMapperCfg() {
        return AbstractWeatherMapper.getClsCsvMapperCfg();
    }

    public static ICsvMapperCfg getClsCsvMapperCfg() {
        return sCfgChk;
    }
}




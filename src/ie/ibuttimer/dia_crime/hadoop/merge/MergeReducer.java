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

package ie.ibuttimer.dia_crime.hadoop.merge;

import ie.ibuttimer.dia_crime.hadoop.AbstractReducer;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.crime.CrimeReducer;
import ie.ibuttimer.dia_crime.hadoop.crime.CrimeWritable;
import ie.ibuttimer.dia_crime.hadoop.crime.IOutputType;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.misc.DateWritable;
import ie.ibuttimer.dia_crime.hadoop.stock.StockWritable;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherReducer;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherWritable;
import ie.ibuttimer.dia_crime.misc.ConfigReader;
import ie.ibuttimer.dia_crime.misc.Utils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static ie.ibuttimer.dia_crime.hadoop.crime.CrimeReducer.formatOutputTypes;
import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.MAP_STRINGIFIER;

/**
 * Reducer to merge crime, weather and stock data to a single output file
 * - input key : date
 * - input value : CSWWrapperWritable wrapper for crime/weather/stock data
 * - output key : date
 * - output value : value string of <property>:<value> separated by ','
 */
public class MergeReducer extends AbstractReducer<DateWritable, CSWWrapperWritable, DateWritable, Text> implements IOutputType {

    public static final String CRIME_WEATHER_STOCK = "csw";
    public static final String CRIME_WEATHER = "cw";
    public static final String CRIME_STOCK = "cs";
    public static final List<String> MERGE_SECTIONS = List.of(
        CRIME_WEATHER_STOCK, CRIME_WEATHER, CRIME_STOCK);

    private Map<String, OpTypeEntry> categorySet;

    private DateTimeFormatter keyOutDateTimeFormatter;

    /* ValueCache is used to cache incomplete data entries, so they can be completed which the info is available.
        e.g. there is no stock data for non-working days the a fill forward approach is used to populate non-working
        days with the data from the last previous working day.
     */
    protected static ValueCache<
            Long,                   // key: epoch day
            Map<String, Object>,    // cache value: stock map
            List<MultipleOutputsWriteEntry<DateWritable, Map<String, Object>>>  // required cache: write entry less missing cache value
        > cache = new ValueCache<>(
        Long::compareTo,
        k -> k - 1,     // previous epoch day
        k -> k + 1      // next epoch day
    );

    private Counters.ReducerCounter counter;
    private Counters.ReducerCounter dayInCounter;
    private Counters.ReducerCounter dayOutCounter;

    private MultipleOutputs<DateWritable, Text> mos;

    public MergeReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        setLogger(getClass());
        categorySet = newOutputTypeMap();

        // stocks and weather fields are known in advance so add to categories

        // add weather fields to categories
        putOutputTypes(new WeatherWritable().toMap(), WEATHER_PROP_SECTION);

        // add stock fields for individual stocks to categories
        ConfigReader cfgReader = new ConfigReader(STOCK_PROP_SECTION);
        Configuration conf = context.getConfiguration();
        String idList = cfgReader.getConfigProperty(conf, ID_LIST_PROP, "");
        Arrays.asList(idList.split(",")).forEach(s -> {
            StockWritable sw = new StockWritable();
            sw.setId(s);
            sw.toMap().entrySet().stream()
                .filter(es -> !es.getKey().equals(ID_PROP))
                .forEach(es -> {
                    putOutputType(genStockOutputKey(s, es.getKey()), es.getValue().getClass(), STOCK_PROP_SECTION);
                });
        });

        /* set date time formatter for output key, could use any weather/crime/stock section */
        cfgReader.setSection(WEATHER_PROP_SECTION);
        keyOutDateTimeFormatter = cfgReader.getDateTimeFormatter(conf,
                                        OUT_KEY_DATE_FORMAT_PROP, DateTimeFormatter.ISO_LOCAL_DATE);

        counter = getCounter(context, CountersEnum.MERGE_REDUCER_COUNT);
        dayInCounter = getCounter(context, CountersEnum.MERGE_REDUCER_GROUP_IN_COUNT);
        dayOutCounter = getCounter(context, CountersEnum.MERGE_REDUCER_GROUP_OUT_COUNT);

        mos = new MultipleOutputs<>(context);
    }

    private String genStockOutputKey(String id, String property) {
        return id + "_" + property;
    }

    /* property section to use when getting dates & outputting type info;
        could be any of crime/nasdaq/sp500/dowjones/weather section, they are all the same
    */
    private static final String ALT_SECTION = DOWJONES_PROP_SECTION;

    /**
     * Reduce the values for a key
     * @param key       Key value; date string
     * @param values    Values for the specified key
     * @param context   Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(DateWritable key, Iterable<CSWWrapperWritable> values, Context context) throws IOException, InterruptedException {

        Optional<Long> inCount = dayInCounter.getCount();
        inCount.ifPresent(count -> {
            if (count == 0) {
                Configuration conf = context.getConfiguration();
                getTagStrings(conf, ALT_SECTION).forEach(tagLine -> {
                    List.of(CRIME_WEATHER_STOCK, CRIME_WEATHER, CRIME_STOCK)
                        .forEach(s -> {
                            try {
                                write(mos, s, DateWritable.COMMENT_KEY, new Text(tagLine), s);
                            } catch (IOException | InterruptedException e) {
                                e.printStackTrace();
                            }
                        });
                });
            }
        });

        dayInCounter.increment();

        // split values by type
        List<CrimeWritable> crimeList = new ArrayList<>(1500);
        List<WeatherWritable> weatherList = new ArrayList<>(24);
        List<StockWritable> stockList = new ArrayList<>();

        values.forEach(iter_value -> {
            if (iter_value.getCrime() != null) {
                crimeList.add(iter_value.getCrime());
            } else if (iter_value.getWeather() != null) {
                weatherList.add(iter_value.getWeather());
            } else if (iter_value.getStock() != null) {
                stockList.add(iter_value.getStock());
            } else {
                throw new IllegalArgumentException("Unknown wrapped object: " + iter_value.getClass().getName());
            }
        });

        // reduce crime to daily totals
        Map<String, Integer> crimeMap = CrimeReducer.reduceToTotalsPerCategory(crimeList, counter, this);
        // reduce weather to daily averages
        Map<String, Object> weatherMap = WeatherReducer.reduceToAverages(weatherList, counter);

        Map<String, Object> crimeWeatherStockMap = new TreeMap<>(crimeMap);
        Map<String, Object> crimeWeatherMap = new TreeMap<>(crimeMap);
        Map<String, Object> crimeStockMap = new TreeMap<>(crimeMap);

        // add weather output to maps requiring it
        crimeWeatherStockMap.putAll(weatherMap);
        crimeWeatherMap.putAll(weatherMap);

        // Note: no stock data for non-working day

        // combine stocks to single map
        Map<String, Object> stockMap = new HashMap<>();
        stockList.forEach(stock -> {
            String id = stock.getId();
            stock.toMap().entrySet().stream()
                .filter(es -> !es.getKey().equals(ID_PROP))
                .forEach(es -> {
                    stockMap.put(genStockOutputKey(id, es.getKey()), es.getValue());
                });
        });

        // key is date, use epoch day as ValueCache key
        Long cacheKey = key.getLocalDate().toEpochDay();

        Triple<Long, Map<String, Object>, List<MultipleOutputsWriteEntry<DateWritable, Map<String, Object>>>> cacheResult;
        List<MultipleOutputsWriteEntry<DateWritable, Map<String, Object>>> toWrite = new ArrayList<>();

        // will always have
        toWrite.add(MultipleOutputsWriteEntry.ofNamedBase(CRIME_WEATHER, key, crimeWeatherMap));

        if (stockMap.size() == 0) {
            // missing stock info, so add what we have to required cache
            // Note: make sure to use a copy of the key in the cache, hadoop reuses the one it passes in!!
            List<MultipleOutputsWriteEntry<DateWritable, Map<String, Object>>> toCache = List.of(
                MultipleOutputsWriteEntry.ofNamedBase(CRIME_WEATHER_STOCK, key.copyOf(), crimeWeatherStockMap),
                MultipleOutputsWriteEntry.ofNamedBase(CRIME_STOCK, key.copyOf(), crimeStockMap));
            cacheResult = cache.addRequired(cacheKey, toCache);

            if (cacheResult != null) {
                // required data was already in cache
                toCache.forEach(we -> {
                    we.getValue().putAll(cacheResult.getMiddle());
                    toWrite.add(we);
                });
            }
        } else {
            List.of(Pair.of(CRIME_WEATHER_STOCK, crimeWeatherStockMap),
                    Pair.of(CRIME_STOCK, crimeStockMap))
                .forEach(pair -> {
                    pair.getRight().putAll(stockMap);
                    toWrite.add(MultipleOutputsWriteEntry.ofNamedBase(pair.getLeft(), key, pair.getRight()));
                });

            cacheResult = cache.addCache(cacheKey, stockMap);
            if (cacheResult != null) {
                // this data was previously required, so have 2 writes to do
                cacheResult.getRight().forEach(we -> {
                    we.getValue().putAll(cacheResult.getMiddle());
                    toWrite.add(we);
                });
            }
        }

        writeOutput(toWrite, context);
    }

    protected void writeOutputEntry(MultipleOutputsWriteEntry<DateWritable, Map<String, Object>> toWrite, Context context) {

        // create value string of <property>:<value> separated by ','
        // e.g. 2001-01-01	01A:2, 02:87, 03:41, 04A:28, 04B:44, 05:66, 06:413, 07:60, 08A:43, 08B:252, 10:12, 11:73, 12:7, 14:233, 15:32, 16:5, 17:68, 18:89, 19:2, 20:44, 22:3, 24:4, 26:211, total:1819
        try {
            write(mos, toWrite.getNamedOutput(), toWrite.getKey(),
                new Text(MAP_STRINGIFIER.stringify(toWrite.getValue())), toWrite.getBaseOutputPath());

            dayOutCounter.increment();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected void writeOutput(List<MultipleOutputsWriteEntry<DateWritable, Map<String, Object>>> toWrite, Context context) {
        toWrite.forEach(t -> writeOutputEntry(t, context));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        if (context.getProgress() == 1.0) {

            // save output types
            formatOutputTypes(this).forEach(s -> {
                try {
                    write(mos, TYPES_NAMED_OP, DateWritable.COMMENT_KEY, new Text(s));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });

            // save incomplete entries using min entry in value cache
            // anything added here will end up at the end of the output so not necessarily in chronological order
            cache.getRequiredAsMin().stream()
                .map(t -> {
                    t.getRight().forEach(we -> we.getValue().putAll(t.getMiddle()));
                    return t.getRight();
                })
                .forEach(t -> writeOutput(t, context));

            dayOutCounter.getCount().ifPresent(c -> dayOutCounter.setValue(c / MERGE_SECTIONS.size()));

            Optional<Long> inCount = dayInCounter.getCount();
            Optional<Long> outCount = dayOutCounter.getCount();
            inCount.ifPresent(in -> {
                outCount.ifPresent(out -> {
                    if (!in.equals(out)) {
                        getLogger().warn(Utils.getDialog("ALERT: Day in/out count mismatch: in=" + in +
                            " out=" + out));
                    }
                });
            });
        }

        mos.close();
    }

    @Override
    public Map<String, OpTypeEntry> getOutputTypeMap() {
        return categorySet;
    }

    protected DateWritable getOutKey(Long cacheKey) {
        LocalDate valueDate = LocalDate.ofEpochDay(cacheKey);
        return DateWritable.ofDate(valueDate, keyOutDateTimeFormatter);
    }

    @Override
    protected DateWritable newKey(String key) {
        return DateWritable.ofDate(key);
    }

    @Override
    protected Text newValue(String value) {
        return new Text(value);
    }

}

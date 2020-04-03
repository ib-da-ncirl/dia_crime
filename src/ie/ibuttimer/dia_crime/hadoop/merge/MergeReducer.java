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
import ie.ibuttimer.dia_crime.hadoop.crime.ICrimeReducer;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.stock.StockWritable;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherReducer;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherWritable;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import ie.ibuttimer.dia_crime.misc.Utils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

import static ie.ibuttimer.dia_crime.hadoop.crime.CrimeReducer.saveOutputTypes;
import static ie.ibuttimer.dia_crime.misc.Constants.*;

public class MergeReducer extends AbstractReducer<Text, CSWWrapperWritable, Text, Text> implements ICrimeReducer {

    private Map<String, Class<?>> categorySet;

    private MapStringifier.ElementStringify prependStringifier = MapStringifier.ElementStringify.of(CLASS_VAL_SEPARATOR);

    private DayWritable day = new DayWritable();

    private DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .appendPattern("uuuu-MM-dd")
        .toFormatter();

    protected static ValueCache<
            Long,                           // key: epoch day
            Map<String, Object>,            // cache value: stock map
            Pair<Text, Map<String, Object>> // required cache: write key and weather/crime map
        > cache = new ValueCache<>(
        Long::compareTo,
        k -> k - 1,     // previous epoch day
        k -> k + 1      // next epoch day
    );

    private Counters.ReducerCounter counter;
    private Counters.ReducerCounter dayInCounter;
    private Counters.ReducerCounter dayOutCounter;


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
        putOutputTypes(new WeatherWritable().toMap());

        // add stock fields for individual stocks to categories
        String idList = context.getConfiguration().get(generatePropertyName(STOCK_PROP_SECTION, ID_LIST_PROP), "");
        Arrays.asList(idList.split(",")).forEach(s -> {
            StockWritable sw = new StockWritable();
            sw.setId(s);
            sw.toMap().entrySet().stream()
                .filter(es -> !es.getKey().equals(ID_PROP))
                .forEach(es -> {
                    putOutputType(genStockOutputKey(s, es.getKey()), es.getValue().getClass());
                });
        });

        counter = getCounter(context, CountersEnum.MERGE_REDUCER_COUNT);
        dayInCounter = getCounter(context, CountersEnum.MERGE_REDUCER_GROUP_IN_COUNT);
        dayOutCounter = getCounter(context, CountersEnum.MERGE_REDUCER_GROUP_OUT_COUNT);
    }

    private String genStockOutputKey(String id, String property) {
        return id + "_" + property;
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
    protected void reduce(Text key, Iterable<CSWWrapperWritable> values, Context context) throws IOException, InterruptedException {

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

        // add weather and crime to single output map
        Map<String, Object> entryMap = new TreeMap<>(crimeMap);
        entryMap.putAll(weatherMap);

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
        Long cacheKey = getValueCacheKey(key.toString());

        Triple<Long, Map<String, Object>, Pair<Text, Map<String, Object>>> cacheResult;
        List<Triple<Text, Map<String, Object>, Map<String, Object>>> toWrite = new ArrayList<>();

        if (stockMap.size() == 0) {
            // missing stock info, so add to required cache
            cacheResult = cache.addRequired(cacheKey, Pair.of(key, entryMap));
            if (cacheResult != null) {
                // required data was already in cache
                toWrite.add(Triple.of(key, cacheResult.getMiddle(), entryMap));
            }
        } else {
            toWrite.add(Triple.of(key, stockMap, entryMap));

            cacheResult = cache.addCache(cacheKey, stockMap);
            if (cacheResult != null) {
                // this data was previously required, so have 2 writes to do
                Pair<Text, Map<String, Object>> required = cacheResult.getRight();
                toWrite.add(Triple.of(required.getLeft(), cacheResult.getMiddle(), required.getRight()));
            }
        }

        writeOutput(toWrite, context);
    }

    protected void writeOutputEntry(Triple<Text, Map<String, Object>, Map<String, Object>> toWrite, Context context) {

        Map<String, Object> outMap = new TreeMap<>(toWrite.getMiddle());
        outMap.putAll(toWrite.getRight());

        // create value string of <category>:<count> separated by ',' with <total>:<count> at the end
        // e.g. 2001-01-01	01A:2, 02:87, 03:41, 04A:28, 04B:44, 05:66, 06:413, 07:60, 08A:43, 08B:252, 10:12, 11:73, 12:7, 14:233, 15:32, 16:5, 17:68, 18:89, 19:2, 20:44, 22:3, 24:4, 26:211, total:1819
        try {
            write(context, toWrite.getLeft(), new Text(MapStringifier.stringify(outMap)));

            dayOutCounter.increment();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected void writeOutput(List<Triple<Text, Map<String, Object>, Map<String, Object>>> toWrite, Context context) {
        toWrite.forEach(t -> writeOutputEntry(t, context));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        saveOutputTypes(context,this, getLogger());

        if (context.getProgress() == 1.0) {

            cache.getRequiredAsMin().stream()
                .map(t -> Triple.of(getOutKey(t.getLeft()), t.getMiddle(), t.getRight().getRight()))
                .forEach(t -> writeOutputEntry(t, context));

            Optional<Long> inCount = dayInCounter.getCount();
            Optional<Long> outCount = dayOutCounter.getCount();
            inCount.ifPresent(in -> {
                outCount.ifPresent(out -> {
                    if (!in.equals(out)) {
                        String ls = System.getProperty("line.separator");
                        String banner = "******************************";
                        StringBuilder sb = new StringBuilder(ls);
                        sb.append(banner).append(ls)
                            .append("ALERT: Day in/out count mismatch: in=").append(in).append(" out=").append(out)
                            .append(banner).append(ls);
                        getLogger().warn(sb.toString());
                    }
                });
            });
        }
    }

    @Override
    public Map<String, Class<?>> getOutputTypeMap() {
        return categorySet;
    }

    protected Long getValueCacheKey(String date) {
        LocalDate valueDate = Utils.getDate(date, dateTimeFormatter, getLogger());
        return valueDate.toEpochDay();
    }

    protected Text getOutKey(Long cacheKey) {
        LocalDate valueDate = LocalDate.ofEpochDay(cacheKey);
        return new Text(valueDate.format(dateTimeFormatter));
    }
}

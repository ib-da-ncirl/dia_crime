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

package ie.ibuttimer.dia_crime.hadoop.stock;

import ie.ibuttimer.dia_crime.hadoop.AbstractBaseWritable;
import ie.ibuttimer.dia_crime.hadoop.AbstractCsvEntryMapper;
import ie.ibuttimer.dia_crime.hadoop.ICsvEntryMapperCfg;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Mapper for a stock entry:
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : date
 * - input value : StockEntryWritable
 */
public abstract class AbstractStockEntryMapper
        extends AbstractCsvEntryMapper<Text, MapWritable> implements IAbstractStockEntryMapper {

    public enum StockMapperKey { DATE, STOCK_ID };

    private StockEntryWritable.StockEntryWritableBuilder builder;

    private Map<String, Integer> indices = new HashMap<>();
    private int maxIndex = -1;

    public static final List<String> STOCK_PROPERTY_INDICES = Arrays.asList(
        DATE_PROP, OPEN_PROP, HIGH_PROP, LOW_PROP, CLOSE_PROP, ADJCLOSE_PROP, VOLUME_PROP
    );

    private CounterEnums.MapperCounter counter;

    private IAbstractStockEntryMapper mapperHelper;


    private Text keyOut = new Text();
    private MapWritable mapOut = new MapWritable();

    private Text id;
    private StockMapperKey keyOutType;

    public AbstractStockEntryMapper(String id, StockMapperKey key) {
        this.id = new Text(id);
        this.keyOutType = key;
        setMapperHelper(this);

        this.builder = StockEntryWritable.StockEntryWritableBuilder.getInstance();
    }

    public void setMapperHelper(IAbstractStockEntryMapper mapperHelper) {
        this.mapperHelper = mapperHelper;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration conf = context.getConfiguration();

        for (String prop : STOCK_PROPERTY_INDICES) {
            int index = conf.getInt(prop, -1);
            if (index > maxIndex) {
                maxIndex = index;
            }
            indices.put(prop, index);
        }

        counter = getCounter(context);
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

        if (!skipHeader(key)) {
            /* Date,Open,High,Low,Close,Adj Close,Volume
             */
            String line = value.toString();
            String[] splits = line.split(getSeparator());
            // if the line ends with separators, they are ignored and consequently the num of splits doesn't match
            // num of columns in csv file
            if (splits.length > maxIndex) {
                Pair<Boolean, LocalDate> filterRes = getDateAndFilter(splits[indices.get(DATE_PROP)]);

                if (filterRes.getLeft()) {
                    LocalDate date = filterRes.getRight();

                    AbstractBaseWritable entry = mapperHelper.generateEntry(date, splits, indices);

                    counter.incrementValue(1);

                    writeOutput(context, entry, keyOut, mapOut, id, keyOutType);
                }
            } else {
                getLogger().warn("Line " + key.get() + " ignored, insufficient columns: " + splits.length);
            }
        }
    }

    @Override
    public AbstractBaseWritable generateEntry(LocalDate date, String[] splits, Map<String, Integer> indices) {
        return builder.clear()
                .setLocalDate(date)
                .setOpen(splits[indices.get(OPEN_PROP)])
                .setHigh(splits[indices.get(HIGH_PROP)])
                .setLow(splits[indices.get(LOW_PROP)])
                .setClose(splits[indices.get(CLOSE_PROP)])
                .setAdjClose(splits[indices.get(ADJCLOSE_PROP)])
                .setVolume(splits[indices.get(VOLUME_PROP)])
                .build();
    }

    public void writeOutput(Context context, AbstractBaseWritable entry, Text keyOut, MapWritable mapOut, Text id, StockMapperKey keyOutType) throws IOException, InterruptedException {
        mapperHelper.getWriteOutput(entry, mapOut, id, keyOutType).forEach(pair -> {
            try {
                keyOut.set(pair.getLeft());
                context.write(keyOut, pair.getRight());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public List<Pair<String, MapWritable>> getWriteOutput(AbstractBaseWritable entry, MapWritable mapOut, Text id,
                                                          StockMapperKey keyOutType) {
        mapOut.clear();
        mapOut.put(id, entry);

        String key;
        switch (keyOutType) {
            case DATE:      key = entry.getLocalDate().toString();    break;
            case STOCK_ID:  key = id.toString();                      break;
            default:        throw new RuntimeException("Unknown 'keyOutType':" + keyOutType);
        };
        return List.of(Pair.of(key, mapOut));
    }

    protected abstract CounterEnums.MapperCounter getCounter(Context context);

    private static ICsvEntryMapperCfg sCfgChk = new AbstractCsvEntryMapperCfg() {

        @Override
        public List<Pair<String, String>> getRequiredProps() {
            return List.of(Pair.of(STOCK_TAG_PROP, "stock tag"));
        }

        @Override
        public List<String> getPropertyIndices() {
            return STOCK_PROPERTY_INDICES;
        }
    };

    public static ICsvEntryMapperCfg getCsvEntryMapperCfg() {
        return sCfgChk;
    }


}




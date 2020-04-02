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
import ie.ibuttimer.dia_crime.hadoop.AbstractCsvMapper;
import ie.ibuttimer.dia_crime.hadoop.misc.CounterEnums;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
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
 * - output key : date or stock id
 * - output value : MapWritable<date/id, StockWritable>
 */
public abstract class AbstractStockMapper<VO>
        extends AbstractCsvMapper<Text, VO>
        implements IAbstractStockMapper.IStockEntryKeyGenerator {

    public enum StockMapperKey { DATE, STOCK_ID };

    private Map<String, Integer> indices = new HashMap<>();
    private int maxIndex = -1;

    public static final List<String> STOCK_PROPERTY_INDICES = Arrays.asList(
        DATE_PROP, OPEN_PROP, HIGH_PROP, LOW_PROP, CLOSE_PROP, ADJCLOSE_PROP, VOLUME_PROP
    );

    private CounterEnums.MapperCounter counter;

    private IAbstractStockMapper mapperHelper;

    private Text keyOut = new Text();

    private Text id;
    private StockMapperKey keyOutType;

    public AbstractStockMapper(String id, StockMapperKey key) {
        this.id = new Text(id);
        this.keyOutType = key;
    }

    public void setMapperHelper(IAbstractStockMapper mapperHelper) {
        this.mapperHelper = mapperHelper;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        super.setup(context, STOCK_PROPERTY_INDICES);

        indices = getIndices();
        maxIndex = getMaxIndex();

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

                    AbstractBaseWritable<?> entry = mapperHelper.generateEntry(date, splits, indices);

                    counter.increment();

                    writeOutput(context, entry, keyOut, id, keyOutType);
                }
            } else {
                getLogger().warn("Line " + key.get() + " ignored, insufficient columns: " + splits.length);
            }
        }
    }

    public void writeOutput(Context context, AbstractBaseWritable<?> entry, Text keyOut, Text id,
                            StockMapperKey keyOutType) {
        mapperHelper.getWriteOutput(entry, id, keyOutType, this).forEach(pair -> {
            try {
                keyOut.set(pair.getLeft());
                write(context, keyOut, (VO) pair.getRight());
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    public String getWriteKey(AbstractBaseWritable<?> entry, Text id, StockMapperKey keyOutType) {
        String key;
        switch (keyOutType) {
            case DATE:      key = entry.getLocalDate().toString();    break;
            case STOCK_ID:  key = id.toString();                      break;
            default:        throw new RuntimeException("Unknown 'keyOutType':" + keyOutType);
        };
        return key;
    }

    public Text getId() {
        return id;
    }

    public void setId(Text id) {
        this.id = id;
    }

    protected abstract CounterEnums.MapperCounter getCounter(Context context);

    public static abstract class AbstractStockEntryMapperCfg extends AbstractCsvEntryMapperCfg {

        private static Property tagProp = Property.of(STOCK_TAG_PROP, "stock tag", "");

        @Override
        public List<Property> getAdditionalProps() {
            return List.of(tagProp,
                Property.of(STATS_PATH_PROP, "path for stats output", ""));
        }

        @Override
        public List<Property> getRequiredProps() {
            List<Property> list = super.getRequiredProps();
            list.add(tagProp);
            return list;
        }

        @Override
        public List<String> getPropertyIndices() {
            return STOCK_PROPERTY_INDICES;
        }
    }

}




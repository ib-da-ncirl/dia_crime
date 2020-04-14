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
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.misc.DateWritable;
import ie.ibuttimer.dia_crime.misc.ConfigReader;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Base Mapper for a stock entry:
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : date or stock id
 * - output value : MapWritable<date/id, StockWritable>
 */
public abstract class AbstractStockMapper<VO>
        extends AbstractCsvMapper<DateWritable, VO>
        implements IAbstractStockMapper.IStockEntryKeyGenerator {

    public enum StockMapperKey { DATE, STOCK_ID };

    private Map<String, Integer> indices = new HashMap<>();
    private int maxIndex = -1;

    public static final List<String> STOCK_PROPERTY_INDICES = Arrays.asList(
        DATE_PROP, OPEN_PROP, HIGH_PROP, LOW_PROP, CLOSE_PROP, ADJCLOSE_PROP, VOLUME_PROP
    );

    private Counters.MapperCounter counter;

    private IAbstractStockMapper mapperHelper;

    private final DateWritable keyOut = DateWritable.of();

    private Text id;
    private final StockMapperKey keyOutType;

    private Map<String, Double> factors;

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
        super.initIndices(context, STOCK_PROPERTY_INDICES);

        indices = getIndices();
        maxIndex = getMaxIndex();

        counter = getCounter(context);

        // read list of factors to apply to property values
        factors = new HashMap<>();
        ConfigReader cfgReader = new ConfigReader(getMapperCfg());
        Map<String, String> factorSetting =
            cfgReader.readCommaSeparatedKeyColonValueProperty(context.getConfiguration(), FACTOR_PROP);

        factorSetting.entrySet().stream()
            .filter(es -> getMapperCfg().getPropertyIndices().contains(es.getKey()))
            .forEach(es -> {
                factors.put(es.getKey(), Double.valueOf(es.getValue()));
            });
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

        if (!skip(key, value)) {
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

    public void writeOutput(Context context, AbstractBaseWritable<?> entry, DateWritable keyOut, Text id,
                            StockMapperKey keyOutType) {
        mapperHelper.getWriteOutput(entry, id, keyOutType, this, getKeyOutDateTimeFormatter())
            .forEach(pair -> {
                try {
                    keyOut.set(pair.getLeft());
                    write(context, keyOut, (VO) pair.getRight());
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
    }

    /**
     * Generate the output key
     * @param entry
     * @param id
     * @param keyOutType
     * @param dateTimeFormatter
     * @return
     */
    public DateWritable getWriteKey(AbstractBaseWritable<?> entry, Text id, StockMapperKey keyOutType,
                              DateTimeFormatter dateTimeFormatter) {
        DateWritable key;
        switch (keyOutType) {
            case DATE:      key = DateWritable.ofDate(entry.getLocalDate());    break;
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

    protected Optional<Double> getFactor(String property) {
        if (factors.containsKey(property)) {
            return Optional.of(factors.get(property));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Get factors to apply to property values
     * @return
     */
    protected Map<String, Double> getFactors() {
        return factors;
    }

    protected abstract Counters.MapperCounter getCounter(Context context);

    // configuration object
    public static class StockMapperCfg extends AbstractCsvMapperCfg {

        private static Property tagProp = Property.of(STOCK_TAG_PROP, "stock tag", "");

        public StockMapperCfg(String propertyRoot) {
            super(propertyRoot);
        }

        @Override
        public List<Property> getAdditionalProps() {
            List<Property> list = new ArrayList<>(getPropertyList(List.of(FACTOR_PROP)));
            list.addAll(List.of(tagProp));
            return list;
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




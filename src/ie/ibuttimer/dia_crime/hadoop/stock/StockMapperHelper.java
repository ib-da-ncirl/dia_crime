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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Mapper helper for stocks
 */
public class StockMapperHelper implements IAbstractStockMapper {

    private String id;
    private StockWritable.StockWritableBuilder builder;

    private MapWritable mapOut = new MapWritable();

    public StockMapperHelper(String id, StockWritable.StockWritableBuilder builder) {
        this.id = id;
        this.builder = builder;
    }

    @Override
    public AbstractBaseWritable<?> generateEntry(LocalDate date, String[] splits, Map<String, Integer> indices) {
        return builder.clear()
            .setLocalDate(date)
            .setOpen(splits[indices.get(OPEN_PROP)])
            .setHigh(splits[indices.get(HIGH_PROP)])
            .setLow(splits[indices.get(LOW_PROP)])
            .setClose(splits[indices.get(CLOSE_PROP)])
            .setAdjClose(splits[indices.get(ADJCLOSE_PROP)])
            .setVolume(splits[indices.get(VOLUME_PROP)])
            .setId(id)
            .build();
    }

    @Override
    public List<Pair<String, Writable>> getWriteOutput(AbstractBaseWritable<?> entry, Text id,
                                                       AbstractStockMapper.StockMapperKey keyOutType,
                                                       IStockEntryKeyGenerator keyGenerator,
                                                       DateTimeFormatter keyOutFormatter) {
        mapOut.clear();
        mapOut.put(id, entry);

        String key = keyGenerator.getWriteKey(entry, id, keyOutType, keyOutFormatter);

        return List.of(Pair.of(key, mapOut));
    }
}

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
import ie.ibuttimer.dia_crime.hadoop.merge.CSWWrapperWritable;
import ie.ibuttimer.dia_crime.hadoop.misc.DateWritable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Mapper helper for stocks for merge job
 */
public class StockWrapMapperHelper extends StockMapperHelper {

    private final CSWWrapperWritable wrapOut = new CSWWrapperWritable();

    public StockWrapMapperHelper(String id, StockWritable.StockWritableBuilder builder) {
        super(id, builder);
    }

    @Override
    public List<Pair<DateWritable, Writable>> getWriteOutput(AbstractBaseWritable<?> entry, Text id,
                                                                 AbstractStockMapper.StockMapperKey keyOutType,
                                                                 IStockEntryKeyGenerator keyGenerator,
                                                                 DateTimeFormatter keyOutFormatter) {
        if (entry instanceof StockWritable) {
            wrapOut.setStock((StockWritable)entry);
        }

        DateWritable key = keyGenerator.getWriteKey(entry, id, keyOutType, keyOutFormatter);

        return List.of(Pair.of(key, wrapOut));
    }
}

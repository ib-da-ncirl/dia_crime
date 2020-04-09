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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public interface IAbstractStockMapper {

    /**
     * Generate the object to write from the mapper
     * @param date      Date of entry
     * @param splits    Strings from line
     * @param indices   Indices from config
     * @return  Object to write
     */
    AbstractBaseWritable<?> generateEntry(LocalDate date, String[] splits, Map<String, Integer> indices);


    /**
     * Get the mapper output to write
     * @param entry         Value to output
     * @param id            Id for mapper
     * @param keyOutType    Key type to output
     * @param keyGenerator
     * @param keyOutFormatter
     * @throws IOException
     * @throws InterruptedException
     * @return
     */
    List<Pair<String, Writable>> getWriteOutput(AbstractBaseWritable<?> entry,
                                                Text id, AbstractStockMapper.StockMapperKey keyOutType,
                                                IStockEntryKeyGenerator keyGenerator, DateTimeFormatter keyOutFormatter);

    interface IStockEntryKeyGenerator {
        String getWriteKey(AbstractBaseWritable<?> entry, Text id, AbstractStockMapper.StockMapperKey keyOutType,
                           DateTimeFormatter dateTimeFormatter);
    }

}

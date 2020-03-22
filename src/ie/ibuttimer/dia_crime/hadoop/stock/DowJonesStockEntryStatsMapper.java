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

import ie.ibuttimer.dia_crime.hadoop.ICsvEntryMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.stats.IStats;
import org.apache.log4j.Logger;

/**
 * Mapper to calculate average values for Dow Jones Industrial Average stock entries:
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : date
 * - input value : StockEntryAvgWritable
 */
public class DowJonesStockEntryStatsMapper extends AbstractDowJonesStockEntryMapper<BigStockEntryWritable>
                    implements IStats {

    private static final Logger logger = Logger.getLogger(DowJonesStockEntryStatsMapper.class);


    public DowJonesStockEntryStatsMapper() {
        // use stock id as the key
        super(StockMapperKey.STOCK_ID);

        BigStockEntryWritable.BigStockEntryWritableBuilder builder = BigStockEntryWritable.BigStockEntryWritableBuilder.getInstance();

        setMapperHelper(new StatsMapperHelper(builder));
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    public static ICsvEntryMapperCfg getCsvEntryMapperCfg() {
        return AbstractStockEntryMapper.getCsvEntryMapperCfg();
    }
}




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
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;

import java.io.IOException;

import static ie.ibuttimer.dia_crime.misc.Constants.NASDAQ_ID;

/**
 * Mapper for a Nasdaq stock entry:
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : stock id
 * - output value : MapWritable<id, StockWritable>
 */
public abstract class AbstractNasdaqStockMapper<W extends AbstractBaseWritable<?>, VO>
        extends AbstractStockMapper<VO> {

    public AbstractNasdaqStockMapper(StockMapperKey key) {
        super(NASDAQ_ID, key);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        setMapperHelper(getMapperHelper());
    }

    protected abstract IAbstractStockMapper getMapperHelper();

    @Override
    protected Counters.MapperCounter getCounter(Context context) {
        return getCounter(context, CountersEnum.NASDAQ_STOCK_MAPPER_COUNT);
    }

}




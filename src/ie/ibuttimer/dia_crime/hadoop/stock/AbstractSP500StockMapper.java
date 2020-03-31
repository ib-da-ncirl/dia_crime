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
import ie.ibuttimer.dia_crime.hadoop.misc.CounterEnums;

import static ie.ibuttimer.dia_crime.misc.Constants.SP500_ID;

/**
 * Mapper for a S&P 500 stock entry:
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : date or stock id
 * - output value : MapWritable<date/id, StockWritable>
 */
public abstract class AbstractSP500StockMapper<W extends AbstractBaseWritable<?>, VO>
        extends AbstractStockMapper<VO> {

    public AbstractSP500StockMapper(StockMapperKey key) {
        super(SP500_ID, key);
    }

    @Override
    protected CounterEnums.MapperCounter getCounter(Context context) {
        return new CounterEnums.MapperCounter(context, SP500CountersEnum.class.getName(),
                SP500CountersEnum.MAPPER_COUNT.toString());
    }
}




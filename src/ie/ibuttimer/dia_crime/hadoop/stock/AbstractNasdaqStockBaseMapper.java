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
import ie.ibuttimer.dia_crime.misc.PropertyWrangler;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Mapper for a NASDAQ Composite stock entry:
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : date
 * - output value : MapWritable<date, StockWritable>
 */
public abstract class AbstractNasdaqStockBaseMapper<VO> extends AbstractNasdaqStockMapper<StockWritable, VO> {

    public AbstractNasdaqStockBaseMapper() {
        super(StockMapperKey.DATE);
        setMapperHelper(new StockMapperHelper(
            getId().toString(), StockWritable.StockEntryWritableBuilder.getInstance()));
    }

    private static ICsvEntryMapperCfg sCfgChk = new AbstractStockEntryMapperCfg() {

        private PropertyWrangler propertyWrangler = new PropertyWrangler(NASDAQ_PROP_SECTION);

        @Override
        public String getPropertyRoot() {
            return NASDAQ_PROP_SECTION;
        }

        @Override
        public String getPropertyPath(String propertyName) {
            return propertyWrangler.getPropertyPath(propertyName);
        }
    };

    @Override
    protected ICsvEntryMapperCfg getEntryMapperCfg() {
        return AbstractNasdaqStockBaseMapper.getCsvEntryMapperCfg();
    }

    public static ICsvEntryMapperCfg getCsvEntryMapperCfg() {
        return sCfgChk;
    }
}




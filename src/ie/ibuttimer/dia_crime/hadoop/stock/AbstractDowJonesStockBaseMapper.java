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

import ie.ibuttimer.dia_crime.hadoop.ICsvMapperCfg;

import static ie.ibuttimer.dia_crime.misc.Constants.DOWJONES_PROP_SECTION;

/**
 * Mapper for a Dow Jones Industrial Average stock entry:
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : date
 * - output value : MapWritable<date, StockWritable>
 */
public abstract class AbstractDowJonesStockBaseMapper<VO> extends AbstractDowJonesStockMapper<StockWritable, VO> {

    public AbstractDowJonesStockBaseMapper() {
        super(StockMapperKey.DATE);
    }

    @Override
    protected IAbstractStockMapper getMapperHelper() {
        return new StockMapperHelper(
            getId().toString(),
            StockWritable.StockWritableBuilder.getInstance(getFactors()));
    }

    private static ICsvMapperCfg sCfgChk = new StockMapperCfg(DOWJONES_PROP_SECTION);

    @Override
    public ICsvMapperCfg getMapperCfg() {
        return AbstractDowJonesStockBaseMapper.getClsCsvMapperCfg();
    }

    public static ICsvMapperCfg getClsCsvMapperCfg() {
        return sCfgChk;
    }
}




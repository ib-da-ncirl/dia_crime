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

package ie.ibuttimer.dia_crime.hadoop.weather;

import ie.ibuttimer.dia_crime.hadoop.ICsvEntryMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.merge.CSWWrapperWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Mapper for a weather entry:
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : date
 * - output value : MapWritable<date, WeatherWritable>
 */
public class WeatherWrapMapper extends AbstractWeatherMapper<CSWWrapperWritable> {

    private static final Logger logger = Logger.getLogger(WeatherWrapMapper.class);

    private CSWWrapperWritable wrapOut = new CSWWrapperWritable();

    @Override
    protected void writeOutput(Context context, Text key, WeatherWritable value) throws IOException, InterruptedException {
        wrapOut.setWeather(value);

        // return the day as the key and the weather entry as the value
        write(context, key, wrapOut);
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    public static ICsvEntryMapperCfg getCsvEntryMapperCfg() {
        return AbstractWeatherMapper.getCsvEntryMapperCfg();
    }
}




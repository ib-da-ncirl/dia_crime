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

package ie.ibuttimer.dia_crime.hadoop.merge;

import ie.ibuttimer.dia_crime.hadoop.AbstractBaseWritable;
import ie.ibuttimer.dia_crime.hadoop.crime.CrimeWritable;
import ie.ibuttimer.dia_crime.hadoop.stock.StockWritable;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CSWWrapperWritable implements Writable {

    private CrimeWritable crime;
    private StockWritable stock;
    private WeatherWritable weather;

    public CSWWrapperWritable() {
        init(null, null, null);
    }

    public CSWWrapperWritable(CrimeWritable crime) {
        init(crime, null, null);
    }

    public CSWWrapperWritable(StockWritable stock) {
        init(null, stock, null);
    }

    public CSWWrapperWritable(WeatherWritable weather) {
        init(null, null, weather);
    }

    private void init(CrimeWritable crime, StockWritable stock, WeatherWritable weather) {
        this.crime = crime;
        this.stock = stock;
        this.weather = weather;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        AbstractBaseWritable.writeNullable(dataOutput, crime);
        AbstractBaseWritable.writeNullable(dataOutput, stock);
        AbstractBaseWritable.writeNullable(dataOutput, weather);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        crime = AbstractBaseWritable.readNullable(dataInput, new CrimeWritable());
        stock = AbstractBaseWritable.readNullable(dataInput, new StockWritable());
        weather = AbstractBaseWritable.readNullable(dataInput, new WeatherWritable());
    }

    public static CSWWrapperWritable read(DataInput dataInput) throws IOException {
        CSWWrapperWritable writable = new CSWWrapperWritable();
        writable.readFields(dataInput);
        return writable;
    }

    public CrimeWritable getCrime() {
        return crime;
    }

    public void setCrime(CrimeWritable crime) {
        init(crime, null, null);
    }

    public StockWritable getStock() {
        return stock;
    }

    public void setStock(StockWritable stock) {
        init(null, stock, null);
    }

    public WeatherWritable getWeather() {
        return weather;
    }

    public void setWeather(WeatherWritable weather) {
        init(null, null, weather);
    }

    public boolean isCrime() {
        return crime != null;
    }

    public boolean isStock() {
        return stock != null;
    }

    public boolean isWeather() {
        return weather != null;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("CSWWrapperWritable{");
        if (crime != null) {
            sb.append("crime=").append(crime);
        } else if (stock != null) {
            sb.append("stock=").append(stock);
        } else if (weather != null) {
            sb.append("weather=").append(weather);
        }
        sb.append("}");
        return sb.toString();
    }
}

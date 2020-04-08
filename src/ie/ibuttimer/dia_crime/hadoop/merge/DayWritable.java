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

import ie.ibuttimer.dia_crime.hadoop.crime.CrimeWritable;
import ie.ibuttimer.dia_crime.hadoop.stock.StockBasketWritable;
import ie.ibuttimer.dia_crime.hadoop.stock.StockWritable;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Custom writable for crime, weather and stock
 */
@Deprecated
public class DayWritable implements Writable {

    private CrimeWritable crime;
    private StockBasketWritable stockBasket;
    private WeatherWritable weather;

    public DayWritable() {
        this.crime = new CrimeWritable();
        this.stockBasket = new StockBasketWritable();
        this.weather = new WeatherWritable();
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        crime.write(dataOutput);
        stockBasket.write(dataOutput);
        weather.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        crime = CrimeWritable.read(dataInput);
        stockBasket.readFields(dataInput);
        weather = WeatherWritable.read(dataInput);
    }

    public static DayWritable read(DataInput dataInput) throws IOException {
        DayWritable writable = new DayWritable();
        writable.readFields(dataInput);
        return writable;
    }

    public CrimeWritable getCrime() {
        return crime;
    }

    public void setCrime(CrimeWritable crime) {
        this.crime = crime;
    }

    public StockBasketWritable getStockBasket() {
        return stockBasket;
    }

    public void setStockBasket(StockBasketWritable stockBasket) {
        this.stockBasket = stockBasket;
    }

    public void putStock(String key, StockWritable value) {
        this.stockBasket.put(key, value);
    }

    public WeatherWritable getWeather() {
        return weather;
    }

    public void setWeather(WeatherWritable weather) {
        this.weather = weather;
    }


    public Map<String, String> toFlatMap() {
        Map<String, String> map = new HashMap<>();
        BiConsumer<String, Object> objMapToStrMap = (s, o) -> map.put(s, o.toString());

        crime.toMap().forEach(objMapToStrMap);
        weather.toMap().forEach(objMapToStrMap);
        stockBasket.getBasket().forEach((s, stockEntryWritable) -> stockEntryWritable.toMap().forEach(objMapToStrMap));

        return map;
    }

}

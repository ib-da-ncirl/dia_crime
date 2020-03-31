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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

public class StockBasketWritable implements Writable {

    private Map<String, StockWritable> basket;

    public StockBasketWritable() {
        this.basket = new TreeMap<>();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(basket.size());
        basket.forEach((key, value) -> {
            try {
                Text.writeString(dataOutput, key);
                value.write(dataOutput);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        IntStream.range(0, dataInput.readInt()).forEach(i -> {
            try {
                String key = Text.readString(dataInput);
                StockWritable value = StockWritable.read(dataInput);
                basket.put(key, value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public static StockBasketWritable read(DataInput dataInput) throws IOException {
        StockBasketWritable writable = new StockBasketWritable();
        writable.readFields(dataInput);
        return writable;
    }

    public void put(String key, StockWritable value) {
        basket.put(key, value);
    }

    public StockWritable get(String key) {
        return basket.get(key);
    }

    public boolean containsKey(String key) {
        return basket.containsKey(key);
    }

    public Map<String, StockWritable> getBasket() {
        return basket;
    }
}

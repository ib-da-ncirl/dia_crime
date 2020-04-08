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

package ie.ibuttimer.dia_crime.hadoop.matrix;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Custom writable for matrix multiplication
 */
public class MatrixWritable implements Writable {

    private String id;
    private int row;
    private int col;
    private List<Double> value;

    // Default constructor to allow (de)serialization
    public MatrixWritable() {
        this("", 0, 0);
    }

    public MatrixWritable(String id, int row, int col) {
        this.id = id;
        this.row = row;
        this.col = col;
        this.value = new ArrayList<>();
    }

    public static MatrixWritable of (String id, int row, int col) {
        return new MatrixWritable(id, row, col);
    }

    public static MatrixWritable read(DataInput dataInput) throws IOException {
        MatrixWritable writable = new MatrixWritable();
        writable.readFields(dataInput);
        return writable;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, id);
        dataOutput.writeInt(row);
        dataOutput.writeInt(col);
        dataOutput.writeInt(value.size());
        for (Double aDouble : value) {
            dataOutput.writeDouble(aDouble);
        }
    }

    public MatrixWritable copyOf() {
        MatrixWritable other = of(id, row, col);
        value.forEach(other::addValue);
        return other;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = Text.readString(dataInput);
        this.row = dataInput.readInt();
        this.col = dataInput.readInt();
        int size = dataInput.readInt();
        this.value = new ArrayList<>(Math.max(size, 10));
        while (size > 0) {
            this.value.add(dataInput.readDouble());
            --size;
        }
    }

    public static MatrixWritable readWritable(DataInput dataInput, MatrixWritable obj) throws IOException {
         obj.readFields(dataInput);
         return obj;
    }

    public static MatrixWritable readWritable(DataInput dataInput) throws IOException {
        return MatrixWritable.readWritable(dataInput, new MatrixWritable());
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getRow() {
        return row;
    }

    public void setRow(int row) {
        this.row = row;
    }

    public long getCol() {
        return col;
    }

    public void setCol(int col) {
        this.col = col;
    }

    public List<Double> getValue() {
        return value;
    }

    public void setValue(List<Double> value) {
        this.value = value;
    }

    public void setValue(Double value) {
        this.value = new ArrayList<>();
        addValue(value);
    }

    public boolean addValue(Double value) {
        return this.value.add(value);
    }

    public static void ifInstance(Object value, Consumer<MatrixWritable> action) {
        if (isInstance(value)) {
            action.accept((MatrixWritable)value);
        }
    }

    public static boolean isInstance(Object value) {
        return (value instanceof MatrixWritable);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
            "id='" + id + '\'' +
            ", row=" + row +
            ", col=" + col +
            ", value=" + value +
            '}';
    }


}

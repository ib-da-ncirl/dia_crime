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

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom writable representing a coordinate
 */
public class CoordinateWritable implements WritableComparable<CoordinateWritable> {

    private int row;
    private int col;

    public CoordinateWritable() {
        this(0, 0);
    }

    public CoordinateWritable(int row, int col) {
        this.row = row;
        this.col = col;
    }

    public static CoordinateWritable of(int row, int col) {
        return new CoordinateWritable(row, col);
    }

    public static CoordinateWritable of() {
        return of(0, 0);
    }

    @Override
    public int compareTo(CoordinateWritable other) {
        int result = this.row - other.row;
        if (result == 0) {
            result = this.col - other.col;
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(row);
        dataOutput.writeInt(col);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        row = dataInput.readInt();
        col = dataInput.readInt();
    }

    public int getRow() {
        return row;
    }

    public void setRow(int row) {
        this.row = row;
    }

    public int getCol() {
        return col;
    }

    public void setCol(int col) {
        this.col = col;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
            "row=" + row +
            ", col=" + col +
            '}';
    }
}

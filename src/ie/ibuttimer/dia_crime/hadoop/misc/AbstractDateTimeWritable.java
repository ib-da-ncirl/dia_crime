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

package ie.ibuttimer.dia_crime.hadoop.misc;

import ie.ibuttimer.dia_crime.hadoop.AbstractBaseWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Base class for date writable's
 * @param <T>
 */
public abstract class AbstractDateTimeWritable<T extends AbstractBaseWritable<?>> extends AbstractBaseWritable<T>
    implements WritableComparable<AbstractDateTimeWritable<T>> {

    private String altString;

    private DateTimeFormatter outputFormatter;

    public AbstractDateTimeWritable() {
        super();
        this.outputFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        this.altString = null;
    }

    protected AbstractDateTimeWritable(String altString) {
        this();
        this.altString = altString;
    }

    public void setOutputFormatter(DateTimeFormatter outputFormatter) {
        this.outputFormatter = outputFormatter;
    }

    public DateTimeFormatter getOutputFormatter() {
        return outputFormatter;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeBoolean(altString != null);
        if (altString != null) {
            Text.writeString(dataOutput, altString);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        if (dataInput.readBoolean()) {
            altString = Text.readString(dataInput);
        }
    }

    @Override
    public T copyOf() {
        T copy = getInstance();
        copy.setLocalDateTime(this.getLocalDateTime());
        return copy;
    }

    @Override
    public int compareTo(AbstractDateTimeWritable<T> other) {
        int result;
        if (this.altString == null && other.altString == null) {
            result = this.getLocalDateTime().compareTo(other.getLocalDateTime());
        } else if (this.altString != null && other.altString != null) {
            result = this.altString.compareTo(other.altString);
        } else {
            result = 0;
        }
        return result;
    }

    @Override
    public String toString() {
        String str;
        str = Objects.requireNonNullElseGet(altString, () -> getLocalDateTime().format(getOutputFormatter()));
        return str;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractDateTimeWritable<?> that = (AbstractDateTimeWritable<?>) o;

        if (!Objects.equals(altString, that.altString)) return false;
        if (!Objects.equals(getLocalDateTime(), that.getLocalDateTime())) return false;
        return Objects.equals(outputFormatter, that.outputFormatter);
    }

    @Override
    public int hashCode() {
        int result = altString != null ? altString.hashCode() : 0;
        result = 31 * result + (outputFormatter != null ? outputFormatter.hashCode() : 0);
        return result;
    }
}

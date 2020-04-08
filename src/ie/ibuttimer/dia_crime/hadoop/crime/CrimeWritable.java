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

package ie.ibuttimer.dia_crime.hadoop.crime;

import ie.ibuttimer.dia_crime.hadoop.AbstractBaseWritable;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.Constants.DESCRIPTION_PROP;

/**
 * Custom writable class for crime entries
 */
public class CrimeWritable extends AbstractBaseWritable<CrimeWritable> implements Writable {

    public static List<String> FIELDS;
    static {
        FIELDS = new ArrayList<>(AbstractBaseWritable.FIELDS);
        FIELDS.addAll(Arrays.asList(PRIMARYTYPE_PROP, DESCRIPTION_PROP, LOCATIONDESCRIPTION_PROP, IUCR_PROP,
            FBICODE_PROP));
    }

    private String primaryType;
    private String description;
    private String locationDescription;
    private String iucr;                    // Illinois Uniform Crime Reporting code
    private String fbiCode;                 // FBI code

    /* ID;Case Number;Date;Block;IUCR;Primary Type;Description;Location Description;Arrest;Domestic;Beat;District;
        Ward;Community Area;FBI Code;X Coordinate;Y Coordinate;Year;Updated On;Latitude;Longitude;Location
     */

    // Default constructor to allow (de)serialization
    public CrimeWritable() {
        super();
        this.primaryType = "";
        this.description = "";
        this.locationDescription = "";
        this.iucr = "";
        this.fbiCode = "";
    }

    @Override
    public CrimeWritable getInstance() {
        return new CrimeWritable();
    }

    public static CrimeWritable read(DataInput dataInput) throws IOException {
        CrimeWritable writable = new CrimeWritable();
        writable.readFields(dataInput);
        return writable;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        Text.writeString(dataOutput, primaryType);
        Text.writeString(dataOutput, description);
        Text.writeString(dataOutput, locationDescription);
        Text.writeString(dataOutput, iucr);
        Text.writeString(dataOutput, fbiCode);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        this.primaryType = Text.readString(dataInput);
        this.description = Text.readString(dataInput);
        this.locationDescription = Text.readString(dataInput);
        this.iucr = Text.readString(dataInput);
        this.fbiCode = Text.readString(dataInput);
    }

    public String getPrimaryType() {
        return primaryType;
    }

    public void setPrimaryType(String primaryType) {
        this.primaryType = primaryType;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getLocationDescription() {
        return locationDescription;
    }

    public void setLocationDescription(String locationDescription) {
        this.locationDescription = locationDescription;
    }

    public String getIucr() {
        return iucr;
    }

    public void setIucr(String iucr) {
        this.iucr = iucr;
    }

    public String getFbiCode() {
        return fbiCode;
    }

    public void setFbiCode(String fbiCode) {
        this.fbiCode = fbiCode;
    }

    @Override
    public void set(CrimeWritable other) {
        super.set(other);
        this.primaryType = other.primaryType;
        this.description = other.description;
        this.locationDescription = other.locationDescription;
        this.iucr = other.iucr;
        this.fbiCode = other.fbiCode;
    }

    @Override
    public CrimeWritable copyOf() {
        CrimeWritable other = new CrimeWritable();
        other.set(this);
        return other;
    }

    @Override
    public Optional<Value> getField(String field) {
        Optional<Value> value = super.getField(field);
        if (value.isEmpty()) {
            switch (field) {
                case PRIMARYTYPE_PROP:          value = Value.ofOptional(primaryType);          break;
                case DESCRIPTION_PROP:          value = Value.ofOptional(description);          break;
                case LOCATIONDESCRIPTION_PROP:  value = Value.ofOptional(locationDescription);  break;
                case IUCR_PROP:                 value = Value.ofOptional(iucr);                 break;
                case FBICODE_PROP:              value = Value.ofOptional(fbiCode);              break;
                default:                        value = Value.empty();                          break;
            }
        }
        return value;
    }

    @Override
    public boolean setField(String field, Object value) {
        AtomicBoolean set = new AtomicBoolean(super.setField(field, value));
        if (!set.get()) {
            Value.ifString(value, v -> {
                set.set(true);
                switch (field) {
                    case PRIMARYTYPE_PROP:          setPrimaryType(v);             break;
                    case DESCRIPTION_PROP:          setDescription(v);             break;
                    case LOCATIONDESCRIPTION_PROP:  setLocationDescription(v);     break;
                    case IUCR_PROP:                 setIucr(v);                    break;
                    case FBICODE_PROP:              setFbiCode(v);                 break;
                    default:                        set.set(false);                break;
                }
            });
        }
        return set.get();
    }

    @Override
    public List<String> getFieldsList() {
        return FIELDS;
    }

    public static void ifInstance(Object value, Consumer<CrimeWritable> action) {
        if (isInstance(value)) {
            action.accept((CrimeWritable)value);
        }
    }

    public static boolean isInstance(Object value) {
        return (value instanceof CrimeWritable);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                super.toString() +
                ", primaryType='" + primaryType + '\'' +
                ", description='" + description + '\'' +
                ", locationDescription='" + locationDescription + '\'' +
                ", iucr='" + iucr + '\'' +
                ", fbiCode='" + fbiCode + '\'' +
                '}';
    }

    public static CrimeWritableBuilder getBuilder() {
        return new CrimeWritableBuilder();
    }

    /**
     * Builder class for crime writables
     */
    public static class CrimeWritableBuilder
            extends AbstractBaseWritable.AbstractBaseWritableBuilder<CrimeWritableBuilder, CrimeWritable> {

        private static final Logger logger = Logger.getLogger(CrimeWritableBuilder.class);

        public CrimeWritableBuilder() {
            super(logger);
        }

        public CrimeWritableBuilder setPrimaryType(String primaryType) {
            getWritable().setPrimaryType(primaryType);
            return this;
        }

        public CrimeWritableBuilder setDescription(String description) {
            getWritable().setDescription(description);
            return this;
        }

        public CrimeWritableBuilder setLocationDescription(String locationDescription) {
            getWritable().setLocationDescription(locationDescription);
            return this;
        }

        public CrimeWritableBuilder setIucr(String iucr) {
            getWritable().setIucr(iucr);
            return this;
        }

        public CrimeWritableBuilder setFbiCode(String fbiCode) {
            getWritable().setFbiCode(fbiCode);
            return this;
        }

        @Override
        public CrimeWritableBuilder getThis() {
            return this;
        }

        @Override
        public CrimeWritable getNewWritable() {
            return new CrimeWritable();
        }

        @Override
        public CrimeWritable build() {
            return getWritable();
        }
    }

}

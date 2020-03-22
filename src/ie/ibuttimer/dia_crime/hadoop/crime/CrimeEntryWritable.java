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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CrimeEntryWritable extends AbstractBaseWritable implements Writable {

    private String primaryType;
    private String description;
    private String locationDescription;
    private String iucr;                    // Illinois Uniform Crime Reporting code
    private String fbiCode;                 // FBI code

    /* ID;Case Number;Date;Block;IUCR;Primary Type;Description;Location Description;Arrest;Domestic;Beat;District;
        Ward;Community Area;FBI Code;X Coordinate;Y Coordinate;Year;Updated On;Latitude;Longitude;Location
     */

    // Default constructor to allow (de)serialization
    public CrimeEntryWritable() {
        super();
        this.primaryType = "";
        this.description = "";
        this.locationDescription = "";
        this.iucr = "";
        this.fbiCode = "";
    }

    public static CrimeEntryWritable read(DataInput in) throws IOException {
        CrimeEntryWritable cew = new CrimeEntryWritable();
        cew.readFields(in);
        return cew;
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

    public static CrimeEntryWritableBuilder getBuilder() {
        return new CrimeEntryWritableBuilder();
    }


    public static class CrimeEntryWritableBuilder
            extends AbstractBaseWritable.AbstractBaseWritableBuilder<CrimeEntryWritableBuilder, CrimeEntryWritable> {

        private static final Logger logger = Logger.getLogger(CrimeEntryWritableBuilder.class);

        public CrimeEntryWritableBuilder() {
            super(logger);
        }

        public CrimeEntryWritableBuilder setPrimaryType(String primaryType) {
            getWritable().setPrimaryType(primaryType);
            return this;
        }

        public CrimeEntryWritableBuilder setDescription(String description) {
            getWritable().setDescription(description);
            return this;
        }

        public CrimeEntryWritableBuilder setLocationDescription(String locationDescription) {
            getWritable().setLocationDescription(locationDescription);
            return this;
        }

        public CrimeEntryWritableBuilder setIucr(String iucr) {
            getWritable().setIucr(iucr);
            return this;
        }

        public CrimeEntryWritableBuilder setFbiCode(String fbiCode) {
            getWritable().setFbiCode(fbiCode);
            return this;
        }

        @Override
        public CrimeEntryWritableBuilder getThis() {
            return this;
        }

        @Override
        public CrimeEntryWritable getNewWritable() {
            return new CrimeEntryWritable();
        }

        @Override
        public CrimeEntryWritable build() {
            return getWritable();
        }
    }

}

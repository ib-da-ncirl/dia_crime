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

import ie.ibuttimer.dia_crime.hadoop.AbstractCsvEntryMapper;
import ie.ibuttimer.dia_crime.hadoop.ICsvEntryMapperCfg;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Mapper for a crime entry:
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : date
 * - input value : CrimeEntryWritable
 */
public class CrimeEntryMapper extends AbstractCsvEntryMapper<Text, CrimeEntryWritable> {

    private static final Logger logger = Logger.getLogger(CrimeEntryMapper.class);

    private CrimeEntryWritable.CrimeEntryWritableBuilder builder;

    private Map<String, Integer> indices = new HashMap<>();
    private int maxIndex = -1;

    public static final List<String> CRIME_PROPERTY_INDICES = Arrays.asList(
        DATE_PROP, PRIMARYTYPE_PROP, DESCRIPTION_PROP, LOCATIONDESCRIPTION_PROP, IUCR_PROP, FBICODE_PROP
    );


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration conf = context.getConfiguration();

        // read the element indices from the configuration
        for (String prop : CRIME_PROPERTY_INDICES) {
            int index = conf.getInt(prop, -1);
            if (index > maxIndex) {
                maxIndex = index;
            }
            indices.put(prop, index);
        }

        builder = CrimeEntryWritable.getBuilder();
    }

    /**
     * Map lines from a csv file
     * @param key       Key; line number
     * @param value     Text for specified line in file
     * @param context   Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (!skipHeader(key)) {
            /* ID;Case Number;Date;Block;IUCR;Primary Type;Description;Location Description;Arrest;Domestic;Beat;District;
                Ward;Community Area;FBI Code;X Coordinate;Y Coordinate;Year;Updated On;Latitude;Longitude;Location
             */
            String line = value.toString();
            String[] splits = line.split(getSeparator());
            // if the line ends with separators, they are ignored and consequently the num of splits doesn't match
            // num of columns in csv file
            if (splits.length > maxIndex) {
                int dateIdx = indices.get(DATE_PROP);
                Pair<Boolean, LocalDateTime> filterRes = getDateTimeAndFilter(splits[indices.get(DATE_PROP)]);

                if (filterRes.getLeft()) {
                    LocalDateTime dateTime = filterRes.getRight();

                    CrimeEntryWritable entry = builder.clear()
                            .setLocalDateTime(dateTime)
                            .setPrimaryType(splits[indices.get(PRIMARYTYPE_PROP)])
                            .setDescription(splits[indices.get(DESCRIPTION_PROP)])
                            .setLocationDescription(splits[indices.get(LOCATIONDESCRIPTION_PROP)])
                            .setIucr(splits[indices.get(IUCR_PROP)])
                            .setFbiCode(splits[indices.get(FBICODE_PROP)])
                            .build();

                    // return the day as the key and the crime entry as the value
                    context.write(new Text(dateTime.toLocalDate().toString()), entry);
                }
            } else {
                logger.warn("Line " + key.get() + " ignored, insufficient columns: " + splits.length);
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }


    private static ICsvEntryMapperCfg sCfgChk = new AbstractCsvEntryMapperCfg() {

        @Override
        public List<String> getPropertyIndices() {
            return CRIME_PROPERTY_INDICES;
        }
    };

    public static ICsvEntryMapperCfg getCsvEntryMapperCfg() {
        return sCfgChk;
    }
}




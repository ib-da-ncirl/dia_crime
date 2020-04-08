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

import ie.ibuttimer.dia_crime.hadoop.AbstractCsvMapper;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.ICsvMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Base Mapper for a crime entry. Parses input line and generates a custom writable.
 * - input key : csv file line number
 * - input value : csv file line text
 * - output key : date
 * @param <VO>  output value
 */
public abstract class AbstractCrimeMapper<VO> extends AbstractCsvMapper<Text, VO> {

    private CrimeWritable.CrimeWritableBuilder builder;

    private Map<String, Integer> indices = new HashMap<>();
    private int maxIndex = -1;

    // names of config properties for indices of data in input csv file
    public static final List<String> CRIME_PROPERTY_INDICES = Arrays.asList(
        DATE_PROP, PRIMARYTYPE_PROP, DESCRIPTION_PROP, LOCATIONDESCRIPTION_PROP, IUCR_PROP, FBICODE_PROP
    );

    private final Text keyOut = new Text();

    private Counters.MapperCounter counter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        super.initIndices(context, CRIME_PROPERTY_INDICES);

        indices = getIndices();
        maxIndex = getMaxIndex();

        builder = CrimeWritable.getBuilder();

        counter = getCounter(context, CountersEnum.CRIME_MAPPER_COUNT);
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

        if (!skip(key, value)) {
            /* ID;Case Number;Date;Block;IUCR;Primary Type;Description;Location Description;Arrest;Domestic;Beat;District;
                Ward;Community Area;FBI Code;X Coordinate;Y Coordinate;Year;Updated On;Latitude;Longitude;Location
             */
            String line = value.toString();
            String[] splits = line.split(getSeparator());
            // if the line ends with separators, they are ignored and consequently the num of splits doesn't match
            // num of columns in csv file
            if (splits.length > maxIndex) {
                Pair<Boolean, LocalDateTime> filterRes = getDateTimeAndFilter(splits[indices.get(DATE_PROP)]);

                if (filterRes.getLeft()) {
                    LocalDateTime dateTime = filterRes.getRight();

                    CrimeWritable entry = builder.clear()
                            .setLocalDateTime(dateTime)
                            .setPrimaryType(splits[indices.get(PRIMARYTYPE_PROP)])
                            .setDescription(splits[indices.get(DESCRIPTION_PROP)])
                            .setLocationDescription(splits[indices.get(LOCATIONDESCRIPTION_PROP)])
                            .setIucr(splits[indices.get(IUCR_PROP)])
                            .setFbiCode(splits[indices.get(FBICODE_PROP)])
                            .build();

                    counter.increment();

                    keyOut.set(dateTime.toLocalDate().toString());

                    // return the day as the key and the crime entry as the value
                    writeOutput(context, keyOut, entry);
                }
            } else {
                getLogger().warn("Line " + key.get() + " ignored, insufficient columns: " + splits.length);
            }
        }
    }

    /**
     * Write mapper output
     * @param context
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    protected abstract void writeOutput(Context context, Text key, CrimeWritable value) throws IOException, InterruptedException;

    // Configuration object
    private static ICsvMapperCfg sCfgChk = new AbstractCsvMapperCfg(CRIME_PROP_SECTION) {

        @Override
        public List<String> getPropertyIndices() {
            return CRIME_PROPERTY_INDICES;
        }
    };

    @Override
    public ICsvMapperCfg getEntryMapperCfg() {
        return AbstractCrimeMapper.getClsCsvMapperCfg();
    }

    public static ICsvMapperCfg getClsCsvMapperCfg() {
        return sCfgChk;
    }
}




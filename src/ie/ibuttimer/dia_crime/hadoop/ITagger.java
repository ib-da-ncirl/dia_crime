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

package ie.ibuttimer.dia_crime.hadoop;

import ie.ibuttimer.dia_crime.misc.PropertyWrangler;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

public interface ITagger {

    String DATE_RANGE_TAG = "Date range: ";
    String FACTORS_TAG = "Factors: ";

    default Pair<LocalDate, LocalDate> getDateRange(Configuration conf, String section) {
        PropertyWrangler wrangler = PropertyWrangler.of(section);
        Pair<LocalDate, LocalDate> result;
        try {
            LocalDate start = LocalDate.parse(
                conf.get(wrangler.getPropertyPath(FILTER_START_DATE_PROP), ""), DateTimeFormatter.ISO_LOCAL_DATE);
            LocalDate end = LocalDate.parse(
                conf.get(wrangler.getPropertyPath(FILTER_END_DATE_PROP), ""), DateTimeFormatter.ISO_LOCAL_DATE);
            result = Pair.of(start, end);
        } catch (DateTimeParseException dpte) {
            result = Pair.of(LocalDate.MIN, LocalDate.MIN);
        }
        return result;
    }

    default String getDateRangeString(Configuration conf, String section) {
        PropertyWrangler wrangler = PropertyWrangler.of(section);
        return DATE_RANGE_TAG + conf.get(wrangler.getPropertyPath(FILTER_START_DATE_PROP), "") +
            " to " +
            conf.get(wrangler.getPropertyPath(FILTER_END_DATE_PROP), "");
    }

    default boolean isDateRangeString(String text) {
        return text.startsWith(DATE_RANGE_TAG);
    }

    default Pair<LocalDate, LocalDate> decodeDateRange(String text) {
        Pair<LocalDate, LocalDate> result = null;

        if (isDateRangeString(text)) {
            String[] splits = text.substring(DATE_RANGE_TAG.length()).split("to");
            if (splits.length == 2) {
                try {
                    LocalDate start = LocalDate.parse(splits[0].trim(), DateTimeFormatter.ISO_LOCAL_DATE);
                    LocalDate end = LocalDate.parse(splits[1].trim(), DateTimeFormatter.ISO_LOCAL_DATE);
                    result = Pair.of(start, end);
                } catch (DateTimeParseException dpte) {
                    result = Pair.of(LocalDate.MIN, LocalDate.MIN);
                }
            }
        }
        if (result == null) {
            result = Pair.of(LocalDate.MIN, LocalDate.MIN);
        }
        return result;
    }

    default String getFactorsString(Configuration conf, String section) {
        PropertyWrangler wrangler = PropertyWrangler.of(section);
        return FACTORS_TAG + conf.get(wrangler.getPropertyPath(FACTOR_PROP), "");
    }

    default boolean isFactorsString(String text) {
        return text.startsWith(FACTORS_TAG);
    }

    default List<String> getTagStrings(Configuration conf, String section) {
        return List.of(
            getDateRangeString(conf, section),
            getFactorsString(conf, section)
        );
    }


    default boolean verifyDateRangeTag(Configuration conf, String section, ICsvEntryMapperCfg cfg, String inputTag) {
        Pair<LocalDate, LocalDate> cfgDates = cfg.getDateRange(conf, section);
        Pair<LocalDate, LocalDate> inDates = cfg.decodeDateRange(inputTag);
        if (!cfgDates.getLeft().equals(inDates.getLeft()) || !cfgDates.getRight().equals(inDates.getRight())) {
            throw new IllegalStateException("Input dates [" + inDates.getLeft() + "/" + inDates.getRight() +
                "] do not match configured dates [" + cfgDates.getLeft() + "/" + cfgDates.getRight() + "]");
        }
        return true;
    }

    default boolean verifyFactorsTag(Configuration conf, String section, ICsvEntryMapperCfg cfg, String inputTag) {
        String cfgFactors = getFactorsString(conf, section);
        if (!cfgFactors.equals(inputTag)) {
            throw new IllegalStateException("Input factors [" + inputTag +
                "] do not match configured factors [" + cfgFactors + "]");
        }
        return true;
    }
}
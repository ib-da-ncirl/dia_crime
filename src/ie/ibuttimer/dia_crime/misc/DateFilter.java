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

package ie.ibuttimer.dia_crime.misc;

import org.apache.hadoop.shaded.org.apache.http.util.TextUtils;

import javax.annotation.Nullable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Date filter, parsing date text and providing a filter result
 */
public class DateFilter {

    private LocalDate startDate;
    private LocalDate endDate;

    public DateFilter(@Nullable String startDate, @Nullable String endDate) {
        this.startDate = getDate(startDate);
        this.endDate = getDate(endDate);
    }

    private LocalDate getDate(String dateStr) {
        LocalDate date = null;
        if (!TextUtils.isEmpty(dateStr)) {
            try {
                date = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
            } catch (DateTimeParseException dpte) {
                date = null;
            }
        }
        return date;
    }

    public boolean filter(LocalDate date) {
        boolean ok;
        if ((startDate != null) && (endDate != null)) {
            ok = afterOrEqual(startDate, date) && beforeOrEqual(endDate, date);
        } else if (startDate != null) {
            ok = afterOrEqual(startDate, date);
        } else if (endDate != null) {
            ok = beforeOrEqual(endDate, date);
        } else {
            ok = false;
        }
        return ok;
    }

    public boolean filter(LocalDateTime dateTime) {
        return filter(dateTime.toLocalDate());
    }

    private boolean afterOrEqual(LocalDate limit, LocalDate date) {
        return (date.isEqual(limit) || date.isAfter(limit));
    }

    private boolean beforeOrEqual(LocalDate limit, LocalDate date) {
        return (date.isEqual(limit) || date.isBefore(limit));
    }
}

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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static ie.ibuttimer.dia_crime.misc.Constants.COMMENT_PREFIX;
import static ie.ibuttimer.dia_crime.misc.Utils.getDate;
import static ie.ibuttimer.dia_crime.misc.Utils.getDateTime;

/**
 * A writable Date class
 */
public class DateWritable extends AbstractDateTimeWritable<DateWritable> {

    public static final DateWritable COMMENT_KEY = new DateWritable(COMMENT_PREFIX);
    public static final DateWritable MIN = DateWritable.ofDate(LocalDate.MIN);
    public static final DateWritable MAX = DateWritable.ofDate(LocalDate.MAX);

    public DateWritable() {
        super();
        setOutputFormatter(DateTimeFormatter.ISO_LOCAL_DATE);
    }

    private DateWritable(String altString) {
        super(altString);
    }

    public static DateWritable of() {
        return new DateWritable();
    }

    public static DateWritable ofDateTime(String dateTime, DateTimeFormatter formatter) {
        DateWritable instance = new DateWritable();
        instance.setLocalDateTime(getDateTime(dateTime, formatter));
        return instance;
    }

    public static DateWritable ofDate(String date, DateTimeFormatter formatter) {
        DateWritable instance = new DateWritable();
        instance.setLocalDate(getDate(date, formatter));
        return instance;
    }

    public static DateWritable ofDate(String date) {
        return ofDate(date, DateTimeFormatter.ISO_LOCAL_DATE);
    }

    public static DateWritable ofDateTime(LocalDateTime dateTime) {
        DateWritable instance = new DateWritable();
        instance.setLocalDateTime(dateTime);
        return instance;
    }

    public static DateWritable ofDate(LocalDate date) {
        DateWritable instance = new DateWritable();
        instance.setLocalDate(date);
        return instance;
    }

    public static DateWritable ofDate(LocalDate date, DateTimeFormatter formatter) {
        DateWritable instance = ofDate(date);
        instance.setOutputFormatter(formatter);
        return instance;
    }

    @Override
    public DateWritable getInstance() {
        return new DateWritable();
    }

}

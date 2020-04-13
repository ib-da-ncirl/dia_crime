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
 * A writable date time class
 */
public class DateTimeWritable extends AbstractDateTimeWritable<DateTimeWritable> {

    public static final DateTimeWritable COMMENT_KEY = new DateTimeWritable(COMMENT_PREFIX);
    public static final DateTimeWritable MIN = DateTimeWritable.ofDateTime(LocalDateTime.MIN);
    public static final DateTimeWritable MAX = DateTimeWritable.ofDateTime(LocalDateTime.MAX);

    public DateTimeWritable() {
        super();
    }

    private DateTimeWritable(String altString) {
        super(altString);
    }

    public static DateTimeWritable of() {
        return new DateTimeWritable();
    }

    public static DateTimeWritable ofDateTime(String dateTime, DateTimeFormatter formatter) {
        DateTimeWritable instance = new DateTimeWritable();
        instance.setLocalDateTime(getDateTime(dateTime, formatter));
        return instance;
    }

    public static DateTimeWritable ofDate(String date, DateTimeFormatter formatter) {
        DateTimeWritable instance = new DateTimeWritable();
        instance.setLocalDate(getDate(date, formatter));
        return instance;
    }

    public static DateTimeWritable ofDateTime(LocalDateTime dateTime) {
        DateTimeWritable instance = new DateTimeWritable();
        instance.setLocalDateTime(dateTime);
        return instance;
    }

    public static DateTimeWritable ofDate(LocalDate dateTime) {
        DateTimeWritable instance = new DateTimeWritable();
        instance.setLocalDate(dateTime);
        return instance;
    }

    @Override
    public DateTimeWritable getInstance() {
        return new DateTimeWritable();
    }

}

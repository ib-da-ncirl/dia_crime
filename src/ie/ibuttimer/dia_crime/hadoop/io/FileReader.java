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

package ie.ibuttimer.dia_crime.hadoop.io;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Hadoop file reader class
 */
public class FileReader {

    private FileUtil fileUtil;
    private FSDataInputStream stream;
    private InputStreamReader inputStreamReader;
    private BufferedReader bufferedReader;

    public FileReader(Path path, Configuration conf) {
        fileUtil = new FileUtil(path, conf);
    }

    public FileReader(String path, Configuration conf) {
        this(new Path(path), conf);
    }

    public boolean wasSuccess() throws IOException {
        return fileUtil.wasSuccess();
    }

    public FileReader open(Path filePath) {
        try {
            stream = fileUtil.fileReadOpen(filePath);
        } catch (IOException e) {
            e.printStackTrace();
            close();
        }
        return this;
    }

    public FileReader open(String filename) {
        try {
            stream = fileUtil.fileReadOpen(filename);
        } catch (IOException e) {
            e.printStackTrace();
            close();
        }
        return this;
    }

    public FileReader open() {
        try {
            stream = fileUtil.fileReadOpen();
        } catch (IOException e) {
            e.printStackTrace();
            close();
        }
        return this;
    }

    /**
     * Get all the lines
     * @return
     * @throws IOException
     */
    public List<String> getAllLines() throws IOException {
        return getAllLines(l -> true);
    }

    /**
     * Get all the lines starting with the specified prefix
     * @param prefix
     * @return
     * @throws IOException
     */
    public List<String> getAllLines(String prefix) throws IOException {
        return getAllLines(Collections.singletonList(prefix));
    }

    /**
     * Get all the lines starting with one of the specified prefixes
     * @param prefixes
     * @return
     * @throws IOException
     */
    public List<String> getAllLines(List<String> prefixes) throws IOException {
        return getAllLines(l -> prefixes.stream().anyMatch(Objects.requireNonNull(l)::startsWith));
    }

    /**
     * Get all the lines matching the specified predicate
     * @param filter
     * @return
     * @throws IOException
     */
    public List<String> getAllLines(Predicate<? super String> filter,
                                    Function<? super String, ? extends String> postFilterMap) throws IOException {
        List<String> lines = null;
        resetReader();
        try {
            lines = bufferedReader.lines()
                .filter(filter)
                .map(postFilterMap)
                .collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            close();
        }
        return lines;
    }

    /**
     * Get all the lines matching the specified predicate
     * @param filter
     * @return
     * @throws IOException
     */
    public List<String> getAllLines(Predicate<? super String> filter) throws IOException {
        return getAllLines(filter, l -> l);
    }

    /**
     * Get the last line
     * @return
     * @throws IOException
     */
    public String getLastLine() throws IOException {
        List<String> lines = getAllLines(l -> true);
        String line;
        if (lines.isEmpty()) {
            line = "";
        } else {
            line = lines.get(lines.size() - 1);
        }
        return line;
    }

    /**
     * Init for reading
     * @return
     * @throws IOException
     */
    private void resetReader() throws IOException {
        if (inputStreamReader == null) {
            inputStreamReader = new InputStreamReader(stream, Charsets.UTF_8);
            bufferedReader = new BufferedReader(inputStreamReader);
        } else {
            bufferedReader.reset();
            inputStreamReader.reset();
            stream.seek(0);
        }
    }

    public void close() {
        try {
            if (bufferedReader != null) {
                bufferedReader.close();
                bufferedReader = null;
            }
            if (inputStreamReader != null) {
                inputStreamReader.close();
                inputStreamReader = null;
            }
            if (stream != null) {
                stream.close();
                stream = null;
            }
            if (fileUtil != null) {
                fileUtil.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

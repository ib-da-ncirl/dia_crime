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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * Hadoop file writer class
 */
public class FileWriter {

    private FileUtil fileUtil;
    private FSDataOutputStream stream;
    private OutputStreamWriter outputStreamWriter;
    private BufferedWriter bufferedWriter;

    public FileWriter(Path path, Configuration conf) {
        fileUtil = new FileUtil(path, conf);
    }

    public FileWriter open(Path filePath, boolean overwrite) {
        try {
            stream = fileUtil.fileWriteOpen(filePath, overwrite);
        } catch (IOException e) {
            e.printStackTrace();
            close();
        }
        return this;
    }

    public FileWriter open(String filename, boolean overwrite) {
        try {
            stream = fileUtil.fileWriteOpen(filename, overwrite);
        } catch (IOException e) {
            e.printStackTrace();
            close();
        }
        return this;
    }

    public FileWriter write(List<String> lines) {
        if (fileUtil == null) {
            throw new IllegalStateException("Writer is closed");
        }
        if (outputStreamWriter == null) {
            outputStreamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
            bufferedWriter = new BufferedWriter(outputStreamWriter);
        }

        try {
            lines.forEach(l -> {
                try {
                    bufferedWriter.write(l);
                    bufferedWriter.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            close();
        }
        return this;
    }

    public FileWriter write(String line) {
        return write(Collections.singletonList(line));
    }

    public FileWriter newline() {
        return write(Collections.singletonList(""));
    }

    public void close() {
        try {
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
            if (outputStreamWriter != null) {
                outputStreamWriter.close();
            }
            if (stream != null) {
                stream.close();
            }
            if (fileUtil != null) {
                fileUtil.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

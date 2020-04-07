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
import org.apache.hadoop.fs.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FileUtil {

    private Path path;
    private Configuration conf;
    private FileSystem fileSystem;

    public FileUtil(Path path, Configuration conf) {
        this.path = path;
        this.conf = conf;
        this.fileSystem = null;
    }

    private FileSystem getFileSystem() throws IOException {
        if (fileSystem == null) {
            fileSystem = FileSystem.get(conf);
        }
        return fileSystem;
    }

    public boolean fileExists(String filename) throws IOException {
        return fileExists(new Path(path, filename));
    }

    public boolean fileExists(Path filePath) throws IOException {
        return getFileSystem().exists(filePath);
    }

    public boolean wasSuccess() throws IOException {
        return fileExists("_SUCCESS");
    }

    public FSDataInputStream fileReadOpen(String filename) throws IOException {
        FSDataInputStream stream = null;
        Path filePath = new Path(path, filename);
        if (fileExists(filename)) {
            stream = fileSystem.open(filePath);
        } else {
            throw new FileNotFoundException("File not found: " + filename);
        }
        return stream;
    }

    public FSDataInputStream fileReadOpen() throws IOException {
        FSDataInputStream stream = null;
        if (fileExists(path)) {
            stream = fileSystem.open(path);
        } else {
            throw new FileNotFoundException("File not found: " + path);
        }
        return stream;
    }

    public FSDataOutputStream fileWriteOpen(Path filePath, boolean overwrite) throws IOException {
        if (fileExists(filePath) && !overwrite) {
            throw new InvalidRequestException("Unable to create '" + filePath + "' as it already exists. " +
                    "Specify 'overwrite' as true to overwrite");
        }
        return fileSystem.create(filePath, overwrite);
    }

    public FSDataOutputStream fileWriteOpen(String filename, boolean overwrite) throws IOException {
        return fileWriteOpen(new Path(path, filename), overwrite);
    }

    public FSDataOutputStream fileWriteOpen(String filename) throws IOException {
        return fileWriteOpen(filename, false);
    }

    public FSDataOutputStream fileWriteOpen(boolean overwrite) throws IOException {
        return fileWriteOpen(path, overwrite);
    }

    public FSDataOutputStream fileWriteOpen() throws IOException {
        return fileWriteOpen(path, false);
    }

    public void write(FSDataOutputStream stream, List<String> lines) throws IOException {
        try (OutputStreamWriter outputStreamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
                BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter)) {
            lines.forEach(l -> {
                try {
                    bufferedWriter.write(l);
                    bufferedWriter.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public void write(FSDataOutputStream stream, String line) throws IOException {
        write(stream, Collections.singletonList(line));
    }

    public void newLine(FSDataOutputStream stream) throws IOException {
        write(stream, Collections.singletonList(""));
    }

    public List<String> read(FSDataInputStream stream) throws IOException {
        List<String> contents = List.of();
        try (InputStreamReader inputStreamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {

            contents = bufferedReader.lines().collect(Collectors.toList());
        }
        return contents;
    }


    public void close() throws IOException {
        fileSystem.close();
    }


    public static class FileWriter {

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

}

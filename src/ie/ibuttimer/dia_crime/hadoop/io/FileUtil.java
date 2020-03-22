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

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    public FSDataOutputStream fileWriteOpen(Path filePath, boolean overwrite) throws IOException {
        FSDataOutputStream stream = null;
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

    public void close() throws IOException {
        fileSystem.close();
    }

}

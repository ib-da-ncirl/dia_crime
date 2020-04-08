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

package ie.ibuttimer.dia_crime.hadoop.matrix;

import ie.ibuttimer.dia_crime.hadoop.AbstractCsvMapper;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.ICsvMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.misc.ConfigReader;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Base class for matrix multiplication mapper
 */
public abstract class MatrixMapper extends AbstractCsvMapper<CoordinateWritable, MatrixWritable> {

    public static final int SPEC_ELEMENT_IDX = 0;
    public static final int SPEC_ROWS_IDX = 1;
    public static final int SPEC_COLS_IDX = 2;
    public static final int SPEC_ITEM_CNT = 3;

    private Counters.MapperCounter counter;

    private MapStringifier.ElementStringify hadoopKeyVal = new MapStringifier.ElementStringify("\t");

    private EqElement mode;
    private Spec specThis;
    private Spec specOther;

    private CoordinateWritable keyOut;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        super.initIndices(context, getEntryMapperCfg().getPropertyIndices());

        keyOut = CoordinateWritable.of();

        setLogger(getClass());

        Configuration conf = context.getConfiguration();

        ConfigReader cfgReader = new ConfigReader(getEntryMapperCfg());

        specThis = new Spec();
        specOther = new Spec();
        List.of(SPEC_PROP, SPEC_OTHER_PROP).forEach(prop -> {
            List<String> vals = cfgReader.readCommaSeparatedProperty(conf, prop);
            if (vals.size() != SPEC_ITEM_CNT) {
                throw new IllegalArgumentException("Incorrect setting for " + prop);
            }
            Spec spec = (prop.equals(SPEC_PROP) ? specThis : specOther);
            spec.element = EqElement.valueOfStr(vals.get(SPEC_ELEMENT_IDX));
            try {
                spec.rows = Integer.parseInt(vals.get(SPEC_ROWS_IDX));
                spec.cols = Integer.parseInt(vals.get(SPEC_COLS_IDX));
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("Incorrect setting for " + prop, nfe);
            }
        });
        mode = specThis.element;
        boolean rowColOk = false;
        switch (mode) {
            case MULTIPLICAND:  rowColOk = (specThis.cols == specOther.rows);   break;
            case MULTIPLIER:    rowColOk = (specThis.rows == specOther.cols);   break;
        }
        if (!rowColOk) {
            throw new IllegalArgumentException("Columns/rows do not match, cannot multiple matrices");
        }

        counter = getCounter(context, CountersEnum.MATRIX_MAPPER_COUNT.name(), mode.name());
    }

    /**
     * Map lines from file
     * @param key       Key; line number
     * @param value     Text for specified line in file
     * @param context   Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (!skipHeader(key) && !skipComment(value) && !skipEmpty(value)) {

            MatrixWritable writable = MatrixWritable.of(specThis.element.name(), 0, 0);
            counter.getCount().ifPresent(c -> writable.setRow(c.intValue()));  // set row index

            /* 1,2,3 */
            List<String> values = readCommaSeparatedString(value.toString());
            if (values.size() != specThis.cols) {
                throw new IllegalStateException(String.format(
                    "Malformed input '%s', number of values [%d] not equal required [%d] for matrix",
                                                value.toString(), values.size(), specThis.cols));
            }

            if (mode.equals(EqElement.MULTIPLICAND)) {
                // output whole row as one, number of 'other' column times
                writable.setCol(Integer.MAX_VALUE); // this one is valid for all columns

                values.forEach(val -> {
                    writable.addValue(Double.parseDouble(val));
                });

                keyOut.setRow(writable.getRow());    // fixed to current row

                for (int col = 0; col < specOther.cols; ++col) {
                    try {
                        keyOut.setCol(col);  // changing col
                        context.write(keyOut, writable);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            } else {    // mode.equals(MULTIPLIER)
                // output individual entries for each column, number of 'other' row times
                AtomicInteger column = new AtomicInteger(0);
                values.forEach(val -> {
                    keyOut.setCol(column.get()); // fixed to current col

                    writable.setCol(column.getAndIncrement());   // set column index
                    writable.setValue(Double.parseDouble(val));

                    for (int row = 0; row < specOther.rows; ++row) {
                        try {
                            keyOut.setRow(row);  // changing row
                            context.write(keyOut, writable);
                        } catch (IOException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            counter.increment();
        }
    }

    // Matrix mapper configuration
    protected static class AbstractMatrixMapperCfg extends AbstractCsvMapperCfg {

        private final Property mSpecProp = Property.of(SPEC_PROP, "specification for matrix file", "");
        private final Property mSpecOtherProp = Property.of(SPEC_OTHER_PROP, "specification for other matrix file", "");

        public AbstractMatrixMapperCfg(String propertyRoot) {
            super(propertyRoot);
        }

        @Override
        public List<Property> getAdditionalProps() {
            return List.of(mSpecProp, mSpecOtherProp);
        }

        @Override
        public List<Property> getRequiredProps() {
            List<Property> list = super.getRequiredProps();
            list.addAll(getAdditionalProps());
            return list;
        }

        @Override
        public List<String> getPropertyIndices() {
            return List.of();
        }
    }

    static final String LHS = "lhs";
    static final String RHS = "rhs";
    enum EqElement {
        MULTIPLICAND(LHS),
        MULTIPLIER(RHS);

        String side;

        EqElement(String side) {
            this.side = side;
        }

        static EqElement valueOfStr(String str) {
            EqElement element = null;
            for (EqElement side : values()) {
                if (side.side.equals(str)) {
                    element = side;
                    break;
                }
            }
            if (element == null) {
                throw new IllegalArgumentException("Unknown " + EqElement.class.getSimpleName() + " string: " + str);
            }
            return element;
        }
    }

    /**
     * Matrix mapper config
     */
    private static class Spec {
        EqElement element;
        int rows;
        int cols;

        Spec() {
            this(EqElement.MULTIPLICAND, 0, 0);
        }

        Spec(EqElement element, int rows, int cols) {
            this.element = element;
            this.rows = rows;
            this.cols = cols;
        }

        static Spec of (EqElement element, int rows, int cols) {
            return new Spec(element, rows, cols);
        }
    }

    /**
     * Matrix mapper class for first matrix in multiplication
     */
    public static class MatrixMapper1 extends MatrixMapper {

        private static final ICsvMapperCfg sCfgChk = new AbstractMatrixMapperCfg(MATRIX_PROP_1_SECTION);

        @Override
        public ICsvMapperCfg getEntryMapperCfg() {
            return getClsCsvMapperCfg();
        }

        public static ICsvMapperCfg getClsCsvMapperCfg() {
            return sCfgChk;
        }
    }

    /**
     * Matrix mapper class for second matrix in multiplication
     */
    public static class MatrixMapper2 extends MatrixMapper {

        private static final ICsvMapperCfg sCfgChk = new AbstractMatrixMapperCfg(MATRIX_PROP_2_SECTION);

        @Override
        public ICsvMapperCfg getEntryMapperCfg() {
            return getClsCsvMapperCfg();
        }

        public static ICsvMapperCfg getClsCsvMapperCfg() {
            return sCfgChk;
        }
    }
}

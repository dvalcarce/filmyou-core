package es.udc.fi.dc.irlab.util;

import org.apache.mahout.math.CardinalityException;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

public class MahoutMathUtils {

    private MahoutMathUtils() {

    }

    /**
     * Overwrite result with the matrix containing the element by element sum of
     * result and other (optimization for dense matrices)
     *
     * @param result
     *            first operand
     * @param other
     *            second operand
     */
    public static void matrixAddInPlace(final Matrix result, final Matrix other) {
        final int rows = result.rowSize();
        final int columns = result.columnSize();

        if (rows != other.rowSize()) {
            throw new CardinalityException(rows, other.rowSize());
        }

        if (columns != other.columnSize()) {
            throw new CardinalityException(columns, other.columnSize());
        }

        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < columns; col++) {
                result.setQuick(row, col, result.getQuick(row, col) + other.getQuick(row, col));
            }
        }
    }

    /**
     * Overwrite result with the vector containing the element by element sum of
     * result and other (optimization for dense vectors)
     *
     * @param result
     *            first operand
     * @param other
     *            second operand
     */
    public static void vectorAddInPlace(final Vector result, final Vector other) {
        final int size = result.size();

        if (size != other.size()) {
            throw new CardinalityException(size, other.size());
        }

        for (int pos = 0; pos < size; pos++) {
            result.setQuick(pos, result.getQuick(pos) + other.getQuick(pos));
        }
    }

}

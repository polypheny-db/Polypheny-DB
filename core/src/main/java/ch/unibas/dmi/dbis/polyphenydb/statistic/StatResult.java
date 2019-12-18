package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.Array;
import java.util.ArrayList;
import lombok.Getter;


/**
 * Contains stats for multiple columns  TODO: evaluate need
 */
public class StatResult {

    @Getter
    private StatQueryColumn[] columns;
    @Getter
    private String[] columnNames;


    public StatResult( StatQueryColumn[] columns ) {
        this.columns = columns;
    }


    /**
     * Constructor which transforms an answer-array into multiple statcolums
     *
     * @param data answer per stat as a two-dimensional array
     */
    public StatResult( ArrayList<String> names, ArrayList<PolySqlType> type, String[][] data ) {

        this.columns = new StatQueryColumn[data[0].length];

        String[][] rotated = rotate2dArray( data );

        for ( int i = 0; i < rotated.length; i++ ) {
            this.columns[i] = new StatQueryColumn( names.get( i ), type.get( i ), rotated[i] );
        }

    }


    /**
     * Rotates a 2d array counterclockwise
     * Assumes 2d array is equally long in all "sub"arrays
     */
    private String[][] rotate2dArray( String[][] data ) {
        int width = data[0].length;
        int height = data.length;

        String[][] rotated = new String[width][height];

        for ( int x = 0; x < width; x++ ) {
            for ( int y = 0; y < height; y++ ) {
                rotated[x][y] = data[y][x];
            }
        }
        return rotated;
    }
}

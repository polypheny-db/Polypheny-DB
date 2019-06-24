/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.sql;


/**
 * Strategy for how NULL values are to be sorted if NULLS FIRST or NULLS LAST are not specified in an item in the ORDER BY clause.
 */
public enum NullCollation {
    /**
     * Nulls first for DESC, nulls last for ASC.
     */
    HIGH,
    /**
     * Nulls last for DESC, nulls first for ASC.
     */
    LOW,
    /**
     * Nulls first for DESC and ASC.
     */
    FIRST,
    /**
     * Nulls last for DESC and ASC.
     */
    LAST;


    /**
     * Returns whether NULL values should appear last.
     *
     * @param desc Whether sort is descending
     */
    public boolean last( boolean desc ) {
        switch ( this ) {
            case FIRST:
                return false;
            case LAST:
                return true;
            case LOW:
                return desc;
            case HIGH:
            default:
                return !desc;
        }
    }


    /**
     * Returns whether a given combination of null direction and sort order is the default order of nulls returned in the ORDER BY clause.
     */
    public boolean isDefaultOrder( boolean nullsFirst, boolean desc ) {
        final boolean asc = !desc;
        final boolean nullsLast = !nullsFirst;

        switch ( this ) {
            case FIRST:
                return nullsFirst;
            case LAST:
                return nullsLast;
            case LOW:
                return (asc && nullsFirst) || (desc && nullsLast);
            case HIGH:
                return (asc && nullsLast) || (desc && nullsFirst);
            default:
                throw new IllegalArgumentException( "Unrecognized Null Collation" );
        }
    }
}


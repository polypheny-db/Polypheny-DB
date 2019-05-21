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

package ch.unibas.dmi.dbis.polyphenydb.webui.models;


import com.google.gson.Gson;


/**
 * Contains data from a query, the titles of the columns and information about the pagination
 */
public class Result<T> {

    /**
     * the header contains information about the columns of a result
     */
    private DbColumn[] header;
    /**
     * the rows containing the fetched data
     */
    private T[][] data;
    /**
     * information for the pagination: what current page is being displayed
     */
    private int currentPage;
    /**
     * information for the pagination: how many pages there can be in total
     */
    private int highestPage;
    /**
     *  table from which the data has been fetched
     */
    private String table;
    /**
     * The request from the UI is being sent back
     * and contains information about which columns are being filtered and which are being sorted
     */
    private UIRequest request;


    public Result( final DbColumn[] header, final T[][] data ) {
        this.header = header;
        this.data = data;
    }


    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson( this );
    }


    public Result setCurrentPage( final int page ) {
        this.currentPage = page;
        return this;
    }


    public Result setHighestPage( final int highestPage ) {
        this.highestPage = highestPage;
        return this;
    }


    public Result setTable ( String table ) {
        this.table = table;
        return this;
    }

}
/*
 * Copyright 2019-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.statistic.exploreByExample;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.SqlProcessor;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.parser.SqlParser;
import org.polypheny.db.sql.parser.SqlParser.SqlParserConfig;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.LimitIterator;
import org.polypheny.db.util.Pair;


@Slf4j
public class ExploreQueryProcessor {

    private final TransactionManager transactionManager;
    private final String databaseName;
    private final String userName;
    private final int pagnation = 200;



    public ExploreQueryProcessor( final TransactionManager transactionManager, String userName, String databaseName) {
        this.transactionManager = transactionManager;
        this.userName = userName;
        this.databaseName = databaseName;

    }
    public ExploreQueryProcessor( final TransactionManager transactionManager, Authenticator authenticator){
        this(transactionManager, "pa", "APP");
    }


    private Transaction getTransaction() {
        try {
            return transactionManager.startTransaction( userName, databaseName, false );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }

    public ExploreQueryResult executeCountSQL(String query){
        ExploreQueryResult result = new ExploreQueryResult(  );
        Transaction transaction = getTransaction();
        try {
            result = executeSqlSelect( transaction, query, pagnation );
            transaction.commit();
        } catch ( ExploreQueryProcessor.QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while executing a query from the console", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }
        return result;
    }



    // -----------------------------------------------------------------------
    //                                Helper
    // -----------------------------------------------------------------------
    private ExploreQueryResult executeSqlSelect( final Transaction transaction, final String sqlSelect ) throws ExploreQueryProcessor.QueryExecutionException {
        return executeSqlSelect( transaction, sqlSelect, getPageSize() );
    }

    private ExploreQueryResult executeSqlSelect( final Transaction transaction, final String sqlSelect, final int pagnation ) throws ExploreQueryProcessor.QueryExecutionException {
        // Parser Config
        SqlParser.ConfigBuilder configConfigBuilder = SqlParser.configBuilder();
        configConfigBuilder.setCaseSensitive( RuntimeConfig.CASE_SENSITIVE.getBoolean() );
        configConfigBuilder.setUnquotedCasing( Casing.TO_LOWER );
        configConfigBuilder.setQuotedCasing( Casing.TO_LOWER );
        SqlParserConfig parserConfig = configConfigBuilder.build();

        PolyphenyDbSignature signature;
        List<List<Object>> rows;
        Iterator<Object> iterator = null;

        try {
            signature = processQuery( transaction, sqlSelect, parserConfig );
            final Enumerable enumerable = signature.enumerable( transaction.getDataContext() );
            //noinspection unchecked

            iterator = enumerable.iterator();

            rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( iterator, pagnation ), new ArrayList<>() );

        } catch ( Throwable t ) {
            if ( iterator != null ) {
                try {
                    ((AutoCloseable) iterator).close();
                } catch ( Exception e ) {
                    log.error( "Exception while closing result iterator", e );
                }
            }
            throw new ExploreQueryProcessor.QueryExecutionException( t );
        }

        try {/*
            List<PolyType> types = new ArrayList<>();
            List<String> names = new ArrayList<>();
            for ( ColumnMetaData metaData : signature.columns ) {

                types.add( PolyType.get( metaData.type.name ) );
                names.add( metaData.schemaName + "." + metaData.tableName + "." + metaData.columnName );
            }*/

            for ( ColumnMetaData metaData : signature.columns ) {
                String columnName = metaData.columnName;

                /*metaData.columnName
                metaData.type.name,
                metaData.schemaName
                metaData.tableName


                 */
            }

            if(rows.size() == 1){
                for(List<Object> row : rows){
                    if (row.size() == 1){
                        for(Object o : row){
                            return new ExploreQueryResult( o.toString(), rows.size() );
                        }
                    }
                }
            }

            List<String[]> data = new ArrayList<>();
            for ( List<Object> row : rows ) {
                String[] temp = new String[row.size()];
                int counter = 0;
                for ( Object o : row ) {
                    if ( o == null ) {
                        temp[counter] = null;
                    } else {
                        temp[counter] = o.toString();
                    }
                    counter++;
                }
                data.add( temp );
            }

            String[][] d = data.toArray( new String[0][] );

            return new ExploreQueryResult( d, rows.size() );
        } finally {
            try {
                ((AutoCloseable) iterator).close();
            } catch ( Exception e ) {
                log.error( "Exception while closing result iterator2", e );
            }
        }
    }


    private PolyphenyDbSignature processQuery( Transaction transaction, String sql, SqlParserConfig parserConfig ) {
        PolyphenyDbSignature signature;
        transaction.resetQueryProcessor();
        SqlProcessor sqlProcessor = transaction.getSqlProcessor( parserConfig );

        SqlNode parsed = sqlProcessor.parse( sql );

        if ( parsed.isA( SqlKind.DDL ) ) {
            signature = sqlProcessor.prepareDdl( parsed );
        } else {
            Pair<SqlNode, RelDataType> validated = sqlProcessor.validate( parsed );
            RelRoot logicalRoot = sqlProcessor.translate( validated.left );

            // Prepare
            signature = transaction.getQueryProcessor().prepareQuery( logicalRoot );
        }
        return signature;
    }


    /**
     * Get the page
     */
    private int getPageSize() {
        return RuntimeConfig.UI_PAGE_SIZE.getInteger();
    }

    /*
    public boolean hasData( String schema, String table, String column ) {
        String query = "SELECT * FROM " + buildQualifiedName( schema, table ) + " LIMIT 1";
        StatisticResult res = executeSqlSelect( query );
        return res.getColumns().length > 0;
    }
    */

    public static String buildQualifiedName( String... strings ) {
        return "\"" + String.join( "\".\"", strings ) + "\"";
    }


    static class QueryExecutionException extends Exception {

        QueryExecutionException( String message ) {
            super( message );
        }

        QueryExecutionException( String message, Exception e ) {
            super( message, e );
        }

        QueryExecutionException( Throwable t ) {
            super( t );
        }
    }


}

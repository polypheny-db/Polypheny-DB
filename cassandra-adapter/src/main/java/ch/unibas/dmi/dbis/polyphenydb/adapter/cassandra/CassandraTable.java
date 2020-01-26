/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
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
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.AbstractQueryableTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTableQueryable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;


/**
 * Table based on a Cassandra column family
 */
public class CassandraTable extends AbstractQueryableTable implements TranslatableTable {

    RelProtoDataType protoRowType;
    Pair<List<String>, List<String>> keyFields;
    List<RelFieldCollation> clusteringOrder;
    private final CassandraSchema schema;
    private final String columnFamily;
    private final boolean view;


    public CassandraTable( CassandraSchema schema, String columnFamily, boolean view ) {
        super( Object[].class );
        this.schema = schema;
        this.columnFamily = columnFamily;
        this.view = view;
    }


    public CassandraTable( CassandraSchema schema, String columnFamily ) {
        this( schema, columnFamily, false );
    }


    public String toString() {
        return "CassandraTable {" + columnFamily + "}";
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        if ( protoRowType == null ) {
            protoRowType = schema.getRelDataType( columnFamily, view );
        }
        return protoRowType.apply( typeFactory );
    }


    public Pair<List<String>, List<String>> getKeyFields() {
        if ( keyFields == null ) {
            keyFields = schema.getKeyFields( columnFamily, view );
        }
        return keyFields;
    }


    public List<RelFieldCollation> getClusteringOrder() {
        if ( clusteringOrder == null ) {
            clusteringOrder = schema.getClusteringOrder( columnFamily, view );
        }
        return clusteringOrder;
    }


    public Enumerable<Object> query( final Session session ) {
        return query( session, ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), 0, -1 );
    }


    /**
     * Executes a CQL query on the underlying table.
     *
     * @param session Cassandra session
     * @param fields List of fields to project
     * @param predicates A list of predicates which should be used in the query
     * @return Enumerator of results
     */
    public Enumerable<Object> query(
            final Session session,
            List<Map.Entry<String, Class>> fields,
            final List<Map.Entry<String, String>> selectFields,
            List<String> predicates,
            List<String> order,
            final Integer offset,
            final Integer fetch ) {
        // Build the type of the resulting row based on the provided fields
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        final RelDataType rowType = getRowType( typeFactory );

        Function1<String, Void> addField = fieldName -> {
            SqlTypeName typeName = rowType.getField( fieldName, true, false ).getType().getSqlTypeName();
            fieldInfo.add( fieldName, typeFactory.createSqlType( typeName ) ).nullable( true );
            return null;
        };

        if ( selectFields.isEmpty() ) {
            for ( Map.Entry<String, Class> field : fields ) {
                addField.apply( field.getKey() );
            }
        } else {
            for ( Map.Entry<String, String> field : selectFields ) {
                addField.apply( field.getKey() );
            }
        }

        final RelProtoDataType resultRowType = RelDataTypeImpl.proto( fieldInfo.build() );

        // Construct the list of fields to project
        final String selectString;
        if ( selectFields.isEmpty() ) {
            selectString = "*";
        } else {
            selectString = Util.toString( () -> {
                final Iterator<Map.Entry<String, String>> selectIterator = selectFields.iterator();

                return new Iterator<String>() {
                    @Override
                    public boolean hasNext() {
                        return selectIterator.hasNext();
                    }


                    @Override
                    public String next() {
                        Map.Entry<String, String> entry = selectIterator.next();
                        return entry.getKey() + " AS " + entry.getValue();
                    }


                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }, "", ", ", "" );
        }

        // Combine all predicates conjunctively
        String whereClause = "";
        if ( !predicates.isEmpty() ) {
            whereClause = " WHERE ";
            whereClause += Util.toString( predicates, "", " AND ", "" );
        }

        // Build and issue the query and return an Enumerator over the results
        StringBuilder queryBuilder = new StringBuilder( "SELECT " );
        queryBuilder.append( selectString );
        queryBuilder.append( " FROM \"" ).append( columnFamily ).append( "\"" );
        queryBuilder.append( whereClause );
        if ( !order.isEmpty() ) {
            queryBuilder.append( Util.toString( order, " ORDER BY ", ", ", "" ) );
        }

        int limit = offset;
        if ( fetch >= 0 ) {
            limit += fetch;
        }
        if ( limit > 0 ) {
            queryBuilder.append( " LIMIT " ).append( limit );
        }
        queryBuilder.append( " ALLOW FILTERING" );
        final String query = queryBuilder.toString();

        return new AbstractEnumerable<Object>() {
            @Override
            public Enumerator<Object> enumerator() {
                final ResultSet results = session.execute( query );
                // Skip results until we get to the right offset
                int skip = 0;
                Enumerator<Object> enumerator = new CassandraEnumerator( results, resultRowType );
                while ( skip < offset && enumerator.moveNext() ) {
                    skip++;
                }
                return enumerator;
            }
        };
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        return new CassandraQueryable<>( dataContext, schema, this, tableName );
    }


    @Override
    public RelNode toRel( RelOptTable.ToRelContext context, RelOptTable relOptTable ) {
        final RelOptCluster cluster = context.getCluster();
        return new CassandraTableScan( cluster, cluster.traitSetOf( CassandraRel.CONVENTION ), relOptTable, this, null );
    }


    /**
     * Implementation of {@link org.apache.calcite.linq4j.Queryable} based on a {@link ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraTable}.
     *
     * @param <T> element type
     */
    public static class CassandraQueryable<T> extends AbstractTableQueryable<T> {

        public CassandraQueryable( DataContext dataContext, SchemaPlus schema, CassandraTable table, String tableName ) {
            super( dataContext, schema, table, tableName );
        }


        @Override
        public Enumerator<T> enumerator() {
            //noinspection unchecked
            final Enumerable<T> enumerable = (Enumerable<T>) getTable().query( getSession() );
            return enumerable.enumerator();
        }


        private CassandraTable getTable() {
            return (CassandraTable) table;
        }


        private Session getSession() {
            return schema.unwrap( CassandraSchema.class ).session;
        }


        /**
         * Called via code-generation.
         *
         * @see ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraMethod#CASSANDRA_QUERYABLE_QUERY
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<Object> query(
                List<Map.Entry<String, Class>> fields,
                List<Map.Entry<String, String>> selectFields,
                List<String> predicates,
                List<String> order,
                Integer offset,
                Integer fetch ) {
            return getTable().query( getSession(), fields, selectFields, predicates, order, offset, fetch );
        }
    }
}


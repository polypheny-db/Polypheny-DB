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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.adapter.mongodb;


import com.mongodb.client.gridfs.GridFSBucket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.Setter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.type.RelRecordType;
import org.polypheny.db.util.Pair;


/**
 * Relational expression that uses Mongo calling convention.
 */
public interface MongoRel extends RelNode {

    void implement( Implementor implementor );

    /**
     * Calling convention for relational operations that occur in MongoDB.
     */
    Convention CONVENTION = MongoConvention.INSTANCE;//new Convention.Impl( "MONGO", MongoRel.class );


    /**
     * Callback for the implementation process that converts a tree of {@link MongoRel} nodes into a MongoDB query.
     */
    class Implementor {

        final List<Pair<String, String>> list = new ArrayList<>();
        public List<String> operations = new ArrayList<>();
        public BsonArray filter = new BsonArray();
        @Getter
        @Setter
        public GridFSBucket bucket;
        public List<BsonDocument> preProjections = new ArrayList<>();

        RelOptTable table;
        @Setter
        @Getter
        public boolean hasProject = false;

        MongoTable mongoTable;
        @Setter
        @Getter
        private boolean isDML;

        @Getter
        @Setter
        private Operation operation;

        @Getter
        @Setter
        List<Object> results;

        @Getter
        private RelRecordType staticRowType;


        public Implementor() {
            isDML = false;
        }


        public Implementor( boolean isDML ) {
            this.isDML = isDML;
        }


        public void add( String findOp, String aggOp ) {
            list.add( Pair.of( findOp, aggOp ) );
        }


        public void visitChild( int ordinal, RelNode input ) {
            assert ordinal == 0;
            ((MongoRel) input).implement( this );
        }


        public void setStaticRowType( RelRecordType staticRowType ) {
            if ( mongoTable != null ) {
                this.staticRowType = MongoRowType.fromRecordType( staticRowType, mongoTable );
            } else {
                this.staticRowType = staticRowType;
            }
        }


        public Map<String, String> getLogicalPhysicalNameMapping() {
            if ( staticRowType == null ) {
                throw new RuntimeException( "Could no come up with mapping as rowtype was not specified" );
            }
            Catalog catalog = Catalog.getInstance();
            CatalogTable table = catalog.getTable( mongoTable.getCatalogTable().id );
            List<String> physicalNames = table.columnIds.stream().map( MongoStore::getPhysicalColumnName ).collect( Collectors.toList() );
            return IntStream
                    .range( 0, table.columnIds.size() )
                    .boxed()
                    .collect( Collectors.toMap( key -> table.getColumnNames().get( key ), physicalNames::get ) );


        }


        public String getPhysicalName( String name ) {
            int index = mongoTable.getCatalogTable().getColumnNames().indexOf( name );
            if ( index != -1 ) {
                return MongoStore.getPhysicalColumnName( mongoTable.getCatalogTable().columnIds.get( index ) );
            }
            throw new RuntimeException( "This column is not part of the table." );
        }


        public String getFilters() {
            BsonDocument filter;
            if ( this.filter.size() == 1 ) {
                filter = this.filter.get( 0 ).asDocument();
            } else if ( this.filter.size() == 0 ) {
                filter = new BsonDocument();
            } else {
                filter = new BsonDocument( "$or", this.filter );
            }

            return filter.toJson( JsonWriterSettings.builder().outputMode( JsonMode.EXTENDED ).build() );
        }


        public List<String> getPreProjections() {
            return preProjections.stream().map( p -> p.toJson( JsonWriterSettings.builder().outputMode( JsonMode.RELAXED ).build() ) ).collect( Collectors.toList() );
        }

    }

}


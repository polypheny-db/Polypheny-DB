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


import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.type.RelRecordType;
import org.polypheny.db.type.BasicPolyType;
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

        RelOptTable table;

        MongoTable mongoTable;
        @Setter
        boolean isDDL;
        @Getter
        @Setter
        List<Object> results;
        @Getter
        final List<Pair<Long, BasicPolyType>> prepFields = new ArrayList<>();
        @Getter
        private RelRecordType rowType;


        public Implementor() {
            isDDL = false;
        }


        public Implementor( boolean isDDL ) {
            this.isDDL = isDDL;
        }


        public void add( String findOp, String aggOp ) {
            list.add( Pair.of( findOp, aggOp ) );
        }


        public void visitChild( int ordinal, RelNode input ) {
            assert ordinal == 0;
            ((MongoRel) input).implement( this );
        }


        public void setRowType( RelRecordType rowType ) {
            if ( mongoTable != null ) {
                this.rowType = MongoRowType.fromRecordType( rowType, mongoTable );
            } else {
                this.rowType = rowType;
            }
        }

    }

}


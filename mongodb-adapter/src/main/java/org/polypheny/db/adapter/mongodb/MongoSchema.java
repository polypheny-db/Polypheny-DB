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


import com.google.common.annotations.VisibleForTesting;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeImpl;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.type.PolyTypeFactoryImpl;


/**
 * Schema mapped onto a directory of MONGO files. Each table in the schema is a MONGO file in that directory.
 */
public class MongoSchema extends AbstractSchema {

    @Getter
    final MongoDatabase mongoDb;

    @Getter
    private final Convention convention = MongoRel.CONVENTION;

    private final Map<String, Table> tableMap;


    /**
     * Creates a MongoDB schema.
     *
     * @param host Mongo host, e.g. "localhost"
     * @param database Mongo database name, e.g. "foodmart"
     * @param tableMap
     */
    //public MongoSchema( String host, String database, List<MongoCredential> credentialsList, MongoClientOptions options ) { // TODO DL: evaluate what options are needed in the end
    public MongoSchema( final String host, final int port, String database, Map<String, Table> tableMap ) {
        super();
        this.tableMap = tableMap;

        try {
            final MongoClient mongo = new MongoClient( host, port );
            this.mongoDb = mongo.getDatabase( database );
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }


    public MongoSchema( final String host, final int port, String database ) {
        this( host, port, database, new HashMap<>() );
    }


    /**
     * Allows tests to inject their instance of the database.
     *
     * @param mongoDb existing mongo database instance
     */
    @VisibleForTesting
    MongoSchema( MongoDatabase mongoDb ) {
        super();
        this.tableMap = new HashMap<>();
        this.mongoDb = Objects.requireNonNull( mongoDb, "mongoDb" );
    }


    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }


    public MongoTable createTable( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        final RelDataTypeFactory typeFactory = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();

        for ( CatalogColumnPlacement placement : columnPlacementsOnStore ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( placement.columnId );
            RelDataType sqlType = catalogColumn.getRelDataType( typeFactory );
            fieldInfo.add( catalogColumn.name, catalogColumn.name, sqlType ).nullable( catalogColumn.nullable );
        }
        MongoTable table = new MongoTable( catalogTable.name, this, RelDataTypeImpl.proto( fieldInfo.build() ) );
        tableMap.put( catalogTable.name, table );
        return table;

    }

}


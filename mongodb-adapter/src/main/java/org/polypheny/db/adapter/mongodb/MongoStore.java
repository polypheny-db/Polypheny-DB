/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.mongodb;

import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.IndexOptions;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.ByteString;
import org.bson.BsonBoolean;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DockerDeployable;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.docker.DockerManager.ContainerBuilder;
import org.polypheny.db.docker.DockerManager.Image;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;

@Slf4j
public class MongoStore extends DataStore implements DockerDeployable {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "MongoDB";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "MongoDB is a document-based and distributed database.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingBoolean( "persistent", false, true, false, false ),
            new AdapterSettingInteger( "port", false, true, false, 27017 )
    );
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_DOCKER_SETTINGS = ImmutableList.of(
            new AdapterSettingString( "dockerUrl", false, true, false, "localhost" )
    );
    private final MongoClient client;
    private final TransactionProvider transactionProvider;
    private MongoSchema currentSchema;


    public MongoStore( int adapterId, String uniqueName, Map<String, String> settings ) {
        super( adapterId, uniqueName, settings, Boolean.parseBoolean( settings.get( "persistent" ) ), true );

        DockerManager.Container container = new ContainerBuilder( getAdapterId(), Image.MONGODB, getUniqueName(), settings.get( "dockerUrl" ) )
                .withMappedPort( Image.MONGODB.getInternalPort(), Integer.parseInt( settings.get( "port" ) ) )
                .withInitCommands( Arrays.asList( "mongod", "--replSet", "test" ) )
                .withAfterCommands( Arrays.asList( "mongo", "--eval", "rs.initiate()" ), 2000 )
                .build();
        DockerManager.getInstance().initialize( container ).start();

        addInformationPhysicalNames();
        enableInformationPage();

        MongoClientSettings mongoSettings = MongoClientSettings
                .builder()
                .applyToClusterSettings( builder ->
                        builder.hosts( Collections.singletonList( new ServerAddress( settings.get( "host" ), Integer.parseInt( settings.get( "port" ) ) ) ) )
                )
                .build();

        this.client = MongoClients.create( mongoSettings );
        this.transactionProvider = new TransactionProvider( this.client );
    }


    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        String[] splits = name.split( "_" );
        String database = splits[0] + "_" + splits[1];
        // TODO DL: physical schema name is null here, when no placement exists yet so we cut it
        currentSchema = new MongoSchema( database, this.client, transactionProvider );
    }


    @Override
    public Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        return currentSchema.createTable( combinedTable, columnPlacementsOnStore, getAdapterId() );
    }


    @Override
    public Schema getCurrentSchema() {
        return this.currentSchema;
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        transactionProvider.startTransaction();
        currentSchema.database.getCollection( getPhysicalTableName( table.id ) ).deleteMany( transactionProvider.getSession(), new Document() );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        transactionProvider.commit();
    }


    @Override
    public void rollback( PolyXid xid ) {
        transactionProvider.rollback();
    }


    @Override
    public List<AdapterSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
    }


    @Override
    public void shutdown() {
        DockerInstance.getInstance().destroyAll( getAdapterId() );

        removeInformationPage();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {

    }


    @Override
    public void createTable( Context context, CatalogTable catalogTable ) {
        Catalog catalog = Catalog.getInstance();

        context.getStatement().getTransaction().registerInvolvedAdapter( this );

        transactionProvider.startTransaction();
        this.currentSchema.database.createCollection( transactionProvider.getSession(), getPhysicalTableName( catalogTable.id ) );

        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnAdapter( getAdapterId(), catalogTable.id ) ) {
            catalog.updateColumnPlacementPhysicalNames(
                    getAdapterId(),
                    placement.columnId,
                    catalogTable.getSchemaName(),
                    catalogTable.name,
                    getPhysicalColumnName( placement.columnId ),
                    true );
        }
    }


    @Override
    public void dropTable( Context context, CatalogTable combinedTable ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        transactionProvider.startTransaction();
        this.currentSchema.database.getCollection( getPhysicalTableName( combinedTable.id ) ).drop( transactionProvider.getSession() );
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        // updates all columns with this field if a default value is provided
        Document field;
        if ( catalogColumn.defaultValue != null ) {
            CatalogDefaultValue defaultValue = catalogColumn.defaultValue;
            BsonValue value;
            if ( catalogColumn.type.getFamily() == PolyTypeFamily.CHARACTER ) {
                value = new BsonString( defaultValue.value );
            } else if ( PolyType.INT_TYPES.contains( catalogColumn.type ) ) {
                value = new BsonInt32( Integer.parseInt( defaultValue.value ) );
            } else if ( PolyType.FRACTIONAL_TYPES.contains( catalogColumn.type ) ) {
                value = new BsonDouble( Double.parseDouble( defaultValue.value ) );
            } else if ( catalogColumn.type.getFamily() == PolyTypeFamily.BOOLEAN ) {
                value = new BsonBoolean( Boolean.parseBoolean( defaultValue.value ) );
            } else if ( catalogColumn.type.getFamily() == PolyTypeFamily.DATE ) {
                try {
                    value = new BsonInt64( new SimpleDateFormat( "yyyy-MM-dd" ).parse( defaultValue.value ).getTime() );
                } catch ( ParseException e ) {
                    throw new RuntimeException( e );
                }
            } else if ( catalogColumn.type.getFamily() == PolyTypeFamily.TIME ) {
                value = new BsonInt32( (int) Time.valueOf( defaultValue.value ).getTime() );
            } else if ( catalogColumn.type.getFamily() == PolyTypeFamily.TIMESTAMP ) {
                value = new BsonInt64( Timestamp.valueOf( defaultValue.value ).getTime() );
            } else if ( catalogColumn.type.getFamily() == PolyTypeFamily.BINARY ) {
                value = new BsonString( ByteString.toString( ByteString.parseBase64( defaultValue.value ), 64 ) );
            } else {
                value = new BsonString( defaultValue.value );
            }
            if ( catalogColumn.collectionsType == PolyType.ARRAY ) {
                throw new RuntimeException( "Default values are not supported for array types" );
            }

            field = new Document().append( getPhysicalColumnName( catalogColumn.id ), value );
        } else {
            field = new Document().append( getPhysicalColumnName( catalogColumn.id ), null );
        }
        Document update = new Document().append( "$set", field );

        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        transactionProvider.startTransaction();
        this.currentSchema.database.getCollection( getPhysicalTableName( catalogTable.id ) ).updateMany( transactionProvider.getSession(), new Document(), update );

        // Add physical name to placement
        catalog.updateColumnPlacementPhysicalNames(
                getAdapterId(),
                catalogColumn.id,
                currentSchema.getDatabase().getName(),
                catalogTable.name,
                getPhysicalColumnName( catalogColumn.id ),
                false );

    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        Document field = new Document().append( getPhysicalColumnName( columnPlacement.columnId ), 1 );
        Document filter = new Document().append( "$unset", field );

        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        transactionProvider.startTransaction();
        this.currentSchema.database.getCollection( getPhysicalTableName( columnPlacement.tableId ) ).updateMany( transactionProvider.getSession(), new Document(), filter );
    }


    @Override
    public void addIndex( Context context, CatalogIndex catalogIndex ) {
        if ( catalogIndex.method.equals( "multikey" ) ) {
            Document doc = new Document();
            for ( String name : catalogIndex.key.getColumnNames() ) {
                doc.append( name, 1 );
            }

            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            transactionProvider.startTransaction();
            this.currentSchema.database.getCollection( getPhysicalTableName( catalogIndex.key.tableId ) ).createIndex( transactionProvider.getSession(), doc, new IndexOptions().name( catalogIndex.name ) );
        }
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex ) {
        if ( catalogIndex.method.equals( "multikey" ) ) {
            Document doc = new Document();
            for ( String name : catalogIndex.key.getColumnNames() ) {
                doc.append( name, 1 );
            }

            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            transactionProvider.startTransaction();
            this.currentSchema.database.getCollection( getPhysicalTableName( catalogIndex.key.tableId ) ).dropIndex( transactionProvider.getSession(), catalogIndex.name );
        }
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType polyType ) {
        // this is not really possible in mongodb, only way would be to reinsert all date, which is not really performant, but could be a possibility
    }


    @Override
    public List<AvailableIndexMethod> getAvailableIndexMethods() {
        List<AvailableIndexMethod> indexMethods = new ArrayList<>();
        indexMethods.add( new AvailableIndexMethod( "multikey", "Multikey Index" ) );
        return indexMethods;
    }


    @Override
    public AvailableIndexMethod getDefaultIndexMethod() {
        // TODO DL: add more index types
        return new AvailableIndexMethod( "multikey", "Multikey Index" );
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( CatalogTable catalogTable ) {
        return null;
    }


    public static String getPhysicalColumnName( long id ) {
        // we can simply use ids as our physical column names as MongoDB allows this
        return "col" + id;
    }


    public static String getPhysicalTableName( long id ) {
        return "tab-" + id;
    }


    @Override
    public List<AdapterSetting> getDockerSettings() {
        return AVAILABLE_DOCKER_SETTINGS;
    }


}

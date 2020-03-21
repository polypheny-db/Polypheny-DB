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

package org.polypheny.db.catalog;


import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.UnknownTypeException;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedDatabase;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedKey;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedSchema;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;

@Slf4j
public class CatalogInfoPage implements PropertyChangeListener {

    private final InformationManager infoManager;
    private final Catalog catalog;
    private final InformationTable databaseInformation;
    private final InformationTable schemaInformation;
    private final InformationTable tableInformation;
    private final InformationTable columnInformation;
    private final InformationTable combinedDatabaseInformation;
    private final InformationTable combinedSchemaInformation;
    private final InformationTable combinedKeyInformation;


    public CatalogInfoPage( Catalog catalog ) {
        this.catalog = catalog;
        infoManager = InformationManager.getInstance();

        InformationPage page = new InformationPage( "catalog", "Catalog" );
        infoManager.addPage( page );

        this.databaseInformation = addCatalogInformationTable( page, "databases", Arrays.asList( "ID", "Name", "Default SchemaID" ) );

        this.schemaInformation = addCatalogInformationTable( page, "schemas", Arrays.asList( "ID", "Name", "DatabaseID", "SchemaType" ) );

        this.tableInformation = addCatalogInformationTable( page, "tables", Arrays.asList( "ID", "Name", "DatabaseID", "SchemaID" ) );

        this.columnInformation = addCatalogInformationTable( page, "columns", Arrays.asList( "ID", "Name", "DatabaseID", "SchemaID", "TableID" ) );

        this.combinedDatabaseInformation = addCatalogInformationTable( page, "Combined Databases", Arrays.asList( "ID", "Name", "Schemas" ) );

        this.combinedSchemaInformation = addCatalogInformationTable( page, "Combined Schemas", Arrays.asList( "ID", "Name", "Tables" ) );

        this.combinedKeyInformation = addCatalogInformationTable( page, "Combined Keys", Arrays.asList( "ID", "Columns" ) );

        addPersistentInfo( page );

        resetCatalogInformation();
        catalog.addObserver( this );

    }


    private InformationTable addCatalogInformationTable( InformationPage page, String name, List<String> titles ) {
        InformationGroup catalogGroup = new InformationGroup( page, name );
        infoManager.addGroup( catalogGroup );
        InformationTable table = new InformationTable( catalogGroup, titles );
        infoManager.registerInformation( table );
        return table;
    }

    private InformationTable addPersistentInfo( InformationPage page ) {
        InformationGroup catalogGroup = new InformationGroup( page, "Persistent");
        infoManager.addGroup( catalogGroup );
        InformationTable table = new InformationTable( catalogGroup, Collections.singletonList( "is persistent" ) );
        infoManager.registerInformation( table );
        table.addRow( catalog.isPersistent ? "✔️" : "❌" );
        return table;
    }


    @Override
    public void propertyChange( PropertyChangeEvent propertyChangeEvent ) {
        resetCatalogInformation();
    }


    private void resetCatalogInformation() {
        databaseInformation.reset();
        schemaInformation.reset();
        tableInformation.reset();
        columnInformation.reset();
        combinedDatabaseInformation.reset();
        combinedSchemaInformation.reset();
        combinedKeyInformation.reset();
        if( catalog == null ) {
            log.error("Catalog not defined in the catalogInformationPage.");
            return;
        }
        try {
            catalog.getDatabases( null ).forEach( d -> {
                databaseInformation.addRow( d.id, d.name, d.defaultSchemaId );
                try {
                    CatalogCombinedDatabase combinedDatabase = catalog.getCombinedDatabase( d.id );
                    combinedDatabaseInformation.addRow( combinedDatabase.getDatabase().id, combinedDatabase.getDatabase().name, combinedDatabase.getSchemas().toString() );
                } catch ( NullPointerException | GenericCatalogException | UnknownSchemaException | UnknownUserException | UnknownDatabaseException | UnknownTableException e ) {
                    e.printStackTrace();
                }

            } );
            catalog.getSchemas( null, null ).forEach( s -> {
                schemaInformation.addRow( s.id, s.name, s.databaseId, s.schemaType );
                try {
                    CatalogCombinedSchema combinedSchema = catalog.getCombinedSchema( s.id );
                    combinedSchemaInformation.addRow( combinedSchema.getSchema().id, combinedSchema.getSchema().name, combinedSchema.getTables().toString() );
                } catch ( NullPointerException | GenericCatalogException | UnknownSchemaException | UnknownUserException | UnknownDatabaseException | UnknownTableException e ) {
                    e.printStackTrace();
                }
            } );
            catalog.getTables( null, null, null ).forEach( t -> {
                tableInformation.addRow( t.id, t.name, t.databaseId, t.schemaId );
            } );
            catalog.getColumns( null, null, null, null ).forEach( c -> {
                columnInformation.addRow( c.id, c.name, c.databaseId, c.schemaId, c.tableId );
            } );

            for ( CatalogKey k : catalog.getKeys() ) {
                CatalogCombinedKey combinedKey = catalog.getCombinedKey( k.id );
                combinedKeyInformation.addRow( combinedKey.getKey().id, combinedKey.toString() );
            }


        } catch ( NullPointerException | GenericCatalogException | UnknownSchemaException | UnknownCollationException | UnknownColumnException | UnknownTypeException | UnknownTableException | UnknownKeyException e ) {
            e.printStackTrace();
        }
    }
}
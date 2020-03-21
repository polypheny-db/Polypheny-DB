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

package org.polypheny.db.test;


import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.polypheny.db.PolySqlType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.CatalogImpl;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedDatabase;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;


public class CatalogTest {

    CatalogImpl catalog;


    @Before
    public void setup() {
        catalog = new CatalogImpl( "testDB", false, false );
        catalog.clear();
    }


    @Test
    public void testInit() {
        assert (catalog != null);
    }


    @Test
    public void testLayout() throws UnknownUserException, UnknownDatabaseException, GenericCatalogException, UnknownSchemaException, UnknownTableException, UnknownColumnException {
        int userId = catalog.addUser( "tester", "" );
        CatalogUser user = catalog.getUser( userId );
        assertEquals( userId, user.id );
        assertEquals( "tester", user.name );

        long databaseId = catalog.addDatabase( "test_db", userId, user.name, 0, "test_schema" );
        CatalogDatabase database = catalog.getDatabase( "test_db" );
        assertEquals( databaseId, database.id );

        long schemaId = catalog.addSchema( "test_schema", databaseId, userId, SchemaType.RELATIONAL );
        CatalogSchema schema = catalog.getSchema( databaseId, "test_schema" );
        assertEquals( schemaId, schema.id );

        long tableId = catalog.addTable( "test_table", schemaId, userId, TableType.TABLE, null );
        CatalogTable table = catalog.getTable( tableId, "test_table" );
        assertEquals( tableId, table.id );

        long columnId = catalog.addColumn( "test_column", tableId, 0, PolySqlType.BIGINT, null, null, false, null );
        CatalogColumn column = catalog.getColumn( tableId, "test_column" );
        assertEquals( columnId, column.id );
    }


    @Test
    public void testCombinedMaps() throws UnknownUserException, UnknownSchemaException, UnknownDatabaseException, GenericCatalogException, UnknownTableException, UnknownColumnException {
        testLayout();
        CatalogCombinedDatabase database = catalog.getCombinedDatabase( 0 );
        assertEquals( database.getDatabase().name, "test_db" );

        assertEquals( database.getSchemas().size(), 1 );
        assertEquals( database.getSchemas().get( 0 ).getTables().size(), 1 );
        assertEquals( database.getSchemas().get( 0 ).getTables().get( 0 ).getColumns().size(), 1 );
    }


    @After
    public void close() {
        catalog.close();
    }


}
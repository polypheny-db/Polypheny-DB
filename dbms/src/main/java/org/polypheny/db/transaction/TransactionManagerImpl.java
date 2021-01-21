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

package org.polypheny.db.transaction;


import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.transaction.PUID.ConnectionId;
import org.polypheny.db.transaction.PUID.NodeId;
import org.polypheny.db.transaction.PUID.Type;
import org.polypheny.db.transaction.PUID.UserId;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;


@Slf4j
public class TransactionManagerImpl implements TransactionManager {

    private ConcurrentHashMap<PolyXid, Transaction> transactions = new ConcurrentHashMap<>();


    public TransactionManagerImpl() {
        InformationManager im = InformationManager.getInstance();
        InformationPage page = new InformationPage( "Transactions" );
        page.fullWidth();
        im.addPage( page );
        InformationGroup runningTransactionsGroup = new InformationGroup( page, "Running Transactions" );
        im.addGroup( runningTransactionsGroup );
        InformationTable runningTransactionsTable = new InformationTable(
                runningTransactionsGroup,
                Arrays.asList( "ID", "XID Hash", "Statements", "Analyze", "Involved Stores", "Origin" ) );
        im.registerInformation( runningTransactionsTable );
        page.setRefreshFunction( () -> {
            runningTransactionsTable.reset();
            transactions.forEach( ( k, v ) -> runningTransactionsTable.addRow(
                    v.getId(),
                    k.toString().hashCode(),
                    v.getNumberOfStatements(),
                    v.isAnalyze(),
                    v.getInvolvedStores().stream().map( Store::getUniqueName ).collect( Collectors.joining( ", " ) ),
                    v.getOrigin() ) );
        } );
    }


    @Override
    public Transaction startTransaction( CatalogUser user, CatalogSchema defaultSchema, CatalogDatabase database, boolean analyze, String origin, MultimediaFlavor flavor ) {
        final NodeId nodeId = (NodeId) PUID.randomPUID( Type.NODE ); // TODO: get real node id -- configuration.get("nodeid")
        final UserId userId = (UserId) PUID.randomPUID( Type.USER ); // TODO: use real user id
        final ConnectionId connectionId = (ConnectionId) PUID.randomPUID( Type.CONNECTION ); // TODO
        PolyXid xid = generateNewTransactionId( nodeId, userId, connectionId );
        transactions.put( xid, new TransactionImpl( xid, this, user, defaultSchema, database, analyze, origin, flavor ) );
        return transactions.get( xid );
    }


    @Override
    public Transaction startTransaction( CatalogUser user, CatalogSchema defaultSchema, CatalogDatabase database, boolean analyze, String origin ) {
        return startTransaction( user, defaultSchema, database, analyze, origin, MultimediaFlavor.DEFAULT );
    }


    @Override
    public Transaction startTransaction( String user, String database, boolean analyze, String origin, MultimediaFlavor flavor ) throws GenericCatalogException, UnknownUserException, UnknownDatabaseException, UnknownSchemaException {
        Catalog catalog = Catalog.getInstance();
        CatalogUser catalogUser = catalog.getUser( user );
        CatalogDatabase catalogDatabase = catalog.getDatabase( database );
        CatalogSchema catalogSchema = catalog.getSchema( catalogDatabase.id, catalogDatabase.defaultSchemaName );
        return startTransaction( catalogUser, catalogSchema, catalogDatabase, analyze, origin, flavor );
    }


    @Override
    public Transaction startTransaction( String user, String database, boolean analyze, String origin ) throws GenericCatalogException, UnknownUserException, UnknownDatabaseException, UnknownSchemaException {
        return startTransaction( user, database, analyze, origin, MultimediaFlavor.DEFAULT );
    }


    @Override
    public void removeTransaction( PolyXid xid ) throws TransactionException {
        if ( !transactions.containsKey( xid ) ) {
            log.warn( "Unknown transaction id: " + xid );
        } else {
            transactions.remove( xid );
        }
    }


    private static PolyXid generateNewTransactionId( final NodeId nodeId, final UserId userId, final ConnectionId connectionId ) {
        return Utils.generateGlobalTransactionIdentifier( nodeId, userId, connectionId, PUID.randomPUID( PUID.Type.TRANSACTION ) );
    }


}

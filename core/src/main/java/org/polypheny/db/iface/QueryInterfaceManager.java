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

package org.polypheny.db.iface;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceException;
import org.polypheny.db.iface.QueryInterface.QueryInterfaceSetting;
import org.polypheny.db.transaction.TransactionManager;
import org.reflections.Reflections;


@Slf4j
public class QueryInterfaceManager {

    private static QueryInterfaceManager INSTANCE;

    private final Map<Integer, QueryInterface> interfaceById = new HashMap<>();
    private final Map<String, QueryInterface> interfaceByName = new HashMap<>();
    private final Map<Integer, Thread> interfaceThreadById = new HashMap<>();

    private final TransactionManager transactionManager;
    private final Authenticator authenticator;


    public static QueryInterfaceManager getInstance() {
        if ( INSTANCE == null ) {
            throw new RuntimeException( "Interface manager has not yet been initialized" );
        }
        return INSTANCE;
    }


    public static void initialize( TransactionManager transactionManager, Authenticator authenticator ) {
        INSTANCE = new QueryInterfaceManager( transactionManager, authenticator );
    }


    private QueryInterfaceManager( TransactionManager transactionManager, Authenticator authenticator ) {
        this.transactionManager = transactionManager;
        this.authenticator = authenticator;
    }


    public QueryInterface getQueryInterface( String uniqueName ) {
        uniqueName = uniqueName.toLowerCase();
        return interfaceByName.get( uniqueName );
    }


    public QueryInterface getQueryInterface( int id ) {
        return interfaceById.get( id );
    }


    public ImmutableMap<String, QueryInterface> getQueryInterfaces() {
        return ImmutableMap.copyOf( interfaceByName );
    }


    public List<QueryInterfaceInformation> getAvailableQueryInterfaceTypes() {
        Reflections reflections = new Reflections( "org.polypheny.db" );
        Set<Class> classes = ImmutableSet.copyOf( reflections.getSubTypesOf( QueryInterface.class ) );
        List<QueryInterfaceInformation> result = new LinkedList<>();
        try {
            //noinspection unchecked
            for ( Class<QueryInterface> clazz : classes ) {
                // Exclude abstract classes
                if ( !Modifier.isAbstract( clazz.getModifiers() ) ) {
                    String name = (String) clazz.getDeclaredField( "INTERFACE_NAME" ).get( null );
                    String description = (String) clazz.getDeclaredField( "INTERFACE_DESCRIPTION" ).get( null );
                    List<QueryInterfaceSetting> settings = (List<QueryInterfaceSetting>) clazz.getDeclaredField( "AVAILABLE_SETTINGS" ).get( null );
                    result.add( new QueryInterfaceInformation( name, description, clazz, settings ) );
                }
            }
        } catch ( NoSuchFieldException | IllegalAccessException e ) {
            throw new RuntimeException( "Something went wrong while retrieving list of available query interface types.", e );
        }
        return result;
    }


    /**
     * Restores query interfaces from catalog
     */
    public void restoreInterfaces( Catalog catalog ) {
        try {
            List<CatalogQueryInterface> interfaces = catalog.getQueryInterfaces();
            for ( CatalogQueryInterface iface : interfaces ) {
                Class<?> clazz = Class.forName( iface.clazz );
                Constructor<?> ctor = clazz.getConstructor( TransactionManager.class, Authenticator.class, int.class, String.class, Map.class );
                QueryInterface instance = (QueryInterface) ctor.newInstance( transactionManager, authenticator, iface.id, iface.name, iface.settings );

                Thread thread = new Thread( instance );
                thread.start();

                try {
                    thread.join();
                } catch ( InterruptedException e ) {
                    log.warn( "Interrupted on join()", e );
                }

                interfaceByName.put( instance.getUniqueName(), instance );
                interfaceById.put( instance.getQueryInterfaceId(), instance );
                interfaceThreadById.put( instance.getQueryInterfaceId(), thread );
            }
        } catch ( GenericCatalogException | NoSuchMethodException | ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException e ) {
            throw new RuntimeException( "Something went wrong while restoring query interfaces from the catalog.", e );
        }
    }


    public QueryInterface addQueryInterface( Catalog catalog, String clazzName, String uniqueName, Map<String, String> settings ) {
        uniqueName = uniqueName.toLowerCase();
        if ( interfaceByName.containsKey( uniqueName ) ) {
            throw new RuntimeException( "There is already a query interface with this unique name" );
        }
        QueryInterface instance;
        try {
            Class<?> clazz = Class.forName( clazzName );
            Constructor<?> ctor = clazz.getConstructor( TransactionManager.class, Authenticator.class, int.class, String.class, Map.class );
            int ifaceId = catalog.addQueryInterface( uniqueName, clazzName, settings );
            instance = (QueryInterface) ctor.newInstance( transactionManager, authenticator, ifaceId, uniqueName, settings );

            Thread thread = new Thread( instance );
            thread.start();

            try {
                thread.join();
            } catch ( InterruptedException e ) {
                log.warn( "Interrupted on join()", e );
            }

            interfaceByName.put( instance.getUniqueName(), instance );
            interfaceById.put( instance.getQueryInterfaceId(), instance );
            interfaceThreadById.put( instance.getQueryInterfaceId(), thread );
        } catch ( ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException | GenericCatalogException e ) {
            throw new RuntimeException( "Something went wrong while adding a new query interface", e );
        }
        return instance;
    }


    public void removeQueryInterface( Catalog catalog, String uniqueName ) {
        uniqueName = uniqueName.toLowerCase();
        if ( !interfaceByName.containsKey( uniqueName ) ) {
            throw new RuntimeException( "Unknown query interface: " + uniqueName );
        }
        try {
            CatalogQueryInterface catalogQueryInterface = catalog.getQueryInterface( uniqueName );

            // TODO: Check if the query interface has any running transactions

            // Shutdown interface
            interfaceByName.get( uniqueName ).shutdown();

            // Remove interfaces from maps
            interfaceById.remove( catalogQueryInterface.id );
            interfaceByName.remove( uniqueName );
            interfaceThreadById.remove( catalogQueryInterface.id );

            // Delete query interface from catalog
            catalog.deleteQueryInterface( catalogQueryInterface.id );
        } catch ( GenericCatalogException | UnknownQueryInterfaceException e ) {
            throw new RuntimeException( "Something went wrong while removing a query interface", e );
        }
    }


    @AllArgsConstructor
    public static class QueryInterfaceInformation {

        public final String name;
        public final String description;
        public final Class clazz;
        public final List<QueryInterfaceSetting> settings;
    }

}
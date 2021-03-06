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

package org.polypheny.db.catalog.entity;


import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;


@EqualsAndHashCode
public final class CatalogSchema implements CatalogEntity, Comparable<CatalogSchema> {

    private static final long serialVersionUID = 6130781950959616712L;

    public final long id;
    @Getter
    public final String name;
    public final long databaseId;
    public final int ownerId;
    public final String ownerName;
    @Getter
    @EqualsAndHashCode.Exclude
    public final SchemaType schemaType;


    public CatalogSchema(
            final long id,
            @NonNull final String name,
            final long databaseId,
            final int ownerId,
            @NonNull final String ownerName,
            @NonNull final SchemaType schemaType ) {
        this.id = id;
        this.name = name;
        this.databaseId = databaseId;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.schemaType = schemaType;
    }


    @SneakyThrows
    public String getDatabaseName() {
        return Catalog.getInstance().getDatabase( databaseId ).name;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{ name, getDatabaseName(), ownerName, CatalogEntity.getEnumNameOrNull( schemaType ) };
    }


    @Override
    public int compareTo( CatalogSchema o ) {
        if ( o != null ) {
            int comp = (int) (this.databaseId - o.databaseId);
            if ( comp == 0 ) {
                return (int) (this.id - o.id);
            } else {
                return comp;
            }

        }
        return -1;
    }


    @RequiredArgsConstructor
    public class PrimitiveCatalogSchema {

        public final String tableSchem;
        public final String tableCatalog;
        public final String owner;
        public final String schemaType;

    }

}

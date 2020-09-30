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

package org.polypheny.db.partition;

import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;

import java.util.List;

//Possible extensions could be range partitioning and hash partitioning
//Need to check if round robin would be sufficient as well or basically just needed to
// distribute workload for LoadBalancing
//Maybe separate partition in the technical-partition itself.
//And meta information about the partiton characteristics of a table
//the latter could maybe be specified in the table as well.
public abstract class AbstractPartitionManager implements PartitionManager{




    @Getter
    protected boolean allowsUnboundPartition;


    //returns the Index of the partition where to place the object
    public abstract long getTargetPartitionId(CatalogTable catalogTable, String columnValue);

    public abstract boolean validatePartitionDistribution(CatalogTable table);

    public abstract boolean probePartitionDistributionChange(CatalogTable catalogTable, int storeId, long columnId);

    public abstract List<CatalogColumnPlacement> getRelevantPlacements(CatalogTable catalogTable,  List<Long> partitionIds);

    public  boolean validatePartitionSetup(List<String> partitionQualifiers, long numPartitions, List<String> partitionNames){

        if ( numPartitions == 0 && partitionNames.size() < 2){
            throw new RuntimeException("Partition Table failed for  Can't specify partition names with less than 2 names");
        }

        return true;
    }

    public abstract boolean allowsUnboundPartition();
}

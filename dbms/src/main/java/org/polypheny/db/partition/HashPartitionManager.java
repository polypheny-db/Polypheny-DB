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

package org.polypheny.db.partition;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumn;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumnType;
import org.polypheny.db.type.PolyType;


@Slf4j
public class HashPartitionManager extends AbstractPartitionManager {

    public static final boolean REQUIRES_UNBOUND_PARTITION_GROUP = false;
    public static final String FUNCTION_TITLE = "HASH";


    @Override
    public long getTargetPartitionId( CatalogTable catalogTable, String columnValue ) {
        long hashValue = columnValue.hashCode() * -1;

        // Don't want any neg. value for now
        if ( hashValue <= 0 ) {
            hashValue *= -1;
        }

        Catalog catalog = Catalog.getInstance();



        //Get designated HASH partition based on number of internal partitions
        int partitionIndex = (int) (hashValue % catalogTable.partitionProperty.partitionIds.size());

        // Finally decide on which partition to put it
        return catalogTable.partitionProperty.partitionIds.get( partitionIndex ) ;
    }


    // Needed when columnPlacements are being dropped
    // HASH Partitioning needs at least one column placement which contains all partitions as a fallback
    @Override
    public boolean probePartitionGroupDistributionChange( CatalogTable catalogTable, int storeId, long columnId ) {
        // Change is only critical if there is only one column left with the characteristics
        int numberOfFullPlacements = getPlacementsWithAllPartitionGroups( columnId, catalogTable.partitionProperty.partitionGroupIds.size() ).size();
        if ( numberOfFullPlacements <= 1 ) {
            Catalog catalog = Catalog.getInstance();
            //Check if this one column is the column we are about to delete
            if ( catalog.getPartitionGroupsOnDataPlacement( storeId, catalogTable.id ).size() == catalogTable.partitionProperty.partitionGroupIds.size() ) {
                return false;
            }
        }

        return true;
    }


    @Override
    public Map<Long, List<CatalogColumnPlacement>> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds ) {
        Catalog catalog = Catalog.getInstance();

        Map <Long, List<CatalogColumnPlacement>> placementDistribution = new HashMap<>();

        if ( partitionIds != null ) {
            for ( long  partitionId : partitionIds ) {

                CatalogPartition catalogPartition = catalog.getPartition( partitionId );
                List<CatalogColumnPlacement> relevantCcps = new ArrayList<>();

                // Find stores with full placements (partitions)
                // Pick for each column the column placement which has full partitioning //SELECT WORST-CASE ergo Fallback
                for ( long columnId : catalogTable.columnIds ) {
                    List<CatalogColumnPlacement> ccps = catalog.getColumnPlacementsByPartitionGroup( catalogTable.id, catalogPartition.partitionGroupId, columnId );
                    if ( !ccps.isEmpty() ) {
                        //get first column placement which contains partition
                        relevantCcps.add( ccps.get( 0 ) );
                        if ( log.isDebugEnabled() ) {
                            log.debug( "{} {} with part. {}", ccps.get( 0 ).adapterUniqueName, ccps.get( 0 ).getLogicalColumnName(), partitionId  );
                        }
                    }
                }
                placementDistribution.put( partitionId, relevantCcps );
            }
        }

        return placementDistribution;
    }


    @Override
    public boolean validatePartitionGroupSetup( List<List<String>> partitionGroupQualifiers, long numPartitionGroups, List<String> partitionGroupNames, CatalogColumn partitionColumn ) {
        super.validatePartitionGroupSetup( partitionGroupQualifiers, numPartitionGroups, partitionGroupNames, partitionColumn );

        if ( !partitionGroupQualifiers.isEmpty() ) {
            throw new RuntimeException( "PartitionType HASH does not support the assignment of values to partitions" );
        }
        if ( numPartitionGroups < 2 ) {
            throw new RuntimeException( "You can't partition a table with less than 2 partitions. You only specified: '" + numPartitionGroups + "'" );
        }

        return true;
    }


    @Override
    public PartitionFunctionInfo getPartitionFunctionInfo() {
        //Dynamic content which will be generated by selected numPartitions
        List<PartitionFunctionInfoColumn> dynamicRows = new ArrayList<>();
        dynamicRows.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( true )
                .modifiable( true )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "" )
                .build() );

        PartitionFunctionInfo uiObject = PartitionFunctionInfo.builder()
                .functionTitle( FUNCTION_TITLE )
                .description( "Partitions data based on a hash function which is automatically applied to the values of the partition column." )
                .sqlPrefix( "WITH (" )
                .sqlSuffix( ")" )
                .rowSeparation( "," )
                .dynamicRows( dynamicRows )
                .headings( new ArrayList<>( Arrays.asList( "Partition Name" ) ) )
                .build();

        return uiObject;
    }


    @Override
    public boolean requiresUnboundPartitionGroup() {
        return REQUIRES_UNBOUND_PARTITION_GROUP;
    }


    @Override
    public boolean supportsColumnOfType( PolyType type ) {
        return true;
    }

}

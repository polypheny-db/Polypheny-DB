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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionGroup;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumn;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumnType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;


@Slf4j
public class ListPartitionManager extends AbstractPartitionManager {

    public static final boolean REQUIRES_UNBOUND_PARTITION_GROUP = true;
    public static final String FUNCTION_TITLE = "LIST";
    public static final List<PolyType> SUPPORTED_TYPES = ImmutableList.of( PolyType.INTEGER, PolyType.BIGINT, PolyType.SMALLINT, PolyType.TINYINT, PolyType.VARCHAR );


    @Override
    public long getTargetPartitionGroupId( CatalogTable catalogTable, String columnValue ) {
        log.debug( "ListPartitionManager" );

        Catalog catalog = Catalog.getInstance();
        long selectedPartitionGroupId = -1;
        long unboundPartitionGroupId = -1;

        for ( long partitionGroupID : catalogTable.partitionGroupIds ) {

            CatalogPartitionGroup catalogPartitionGroup = catalog.getPartitionGroup( partitionGroupID );

            if ( catalogPartitionGroup.isUnbound ) {
                unboundPartitionGroupId = catalogPartitionGroup.id;
            }
            for ( int i = 0; i < catalogPartitionGroup.partitionQualifiers.size(); i++ ) {
                //Could be int
                if ( catalogPartitionGroup.partitionQualifiers.get( i ).equals( columnValue ) ) {
                    if ( log.isDebugEnabled() ) {
                        log.debug( "Found column value: {} on partitionID {} with qualifiers: {}",
                                columnValue,
                                partitionGroupID,
                                catalogPartitionGroup.partitionQualifiers );
                    }
                    selectedPartitionGroupId = catalogPartitionGroup.id;
                    break;
                }
            }

        }
        // If no concrete partition could be identified, report back the unbound/default partition
        if ( selectedPartitionGroupId == -1 ) {
            selectedPartitionGroupId = unboundPartitionGroupId;
        }

        return selectedPartitionGroupId;
    }


    // Needed when columnPlacements are being dropped
    @Override
    public boolean probePartitionGroupDistributionChange( CatalogTable catalogTable, int storeId, long columnId ) {

        Catalog catalog = Catalog.getInstance();

        //TODO Enable following code block without FullPartitionPlacement fallback

        /* try {
            int thresholdCounter = 0;
            boolean validDistribution = false;
            //check for every partition if the column in question has still all partition somewhere even when columnId on Store would be removed
            for (long partitionId : catalogTable.partitionIds) {

                //check if a column is dropped from a store if this column has still other placements with all partitions
                List<CatalogColumnPlacement> ccps = catalog.getColumnPlacementsByPartition(catalogTable.id, partitionId, columnId);
                for ( CatalogColumnPlacement columnPlacement : ccps){
                    if (columnPlacement.storeId != storeId){
                        thresholdCounter++;
                        break;
                    }
                }
                if ( thresholdCounter < 1){
                    return false;
                }
            }

            } catch ( UnknownPartitionException e) {
            throw  new RuntimeException(e);
         }*/

        // TODO can be removed if upper codeblock is enabled
        // change is only critical if there is only one column left with the characteristics
        int numberOfFullPlacements = getPlacementsWithAllPartitionGroups( columnId, catalogTable.numPartitionGroups ).size();
        if ( numberOfFullPlacements <= 1 ) {
            //Check if this one column is the column we are about to delete
            if ( catalog.getPartitionGroupsOnDataPlacement( storeId, catalogTable.id ).size() == catalogTable.numPartitionGroups ) {
                return false;
            }
        }

        return true;

    }


    // Relevant for select
    @Override
    public List<CatalogColumnPlacement> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionGroupIds ) {
        Catalog catalog = Catalog.getInstance();
        List<CatalogColumnPlacement> relevantCcps = new ArrayList<>();

        if ( partitionGroupIds != null ) {
            for ( long partitionGroupId : partitionGroupIds ) {
                // Find stores with full placements (partitions)
                // Pick for each column the column placement which has full partitioning //SELECT WORST-CASE ergo Fallback
                for ( long columnId : catalogTable.columnIds ) {
                    List<CatalogColumnPlacement> ccps = catalog.getColumnPlacementsByPartitionGroup( catalogTable.id, partitionGroupId, columnId );
                    if ( !ccps.isEmpty() ) {
                        //get first column placement which contains partition
                        relevantCcps.add( ccps.get( 0 ) );
                        if ( log.isDebugEnabled() ) {
                            log.debug( "{} {} with part. {}", ccps.get( 0 ).adapterUniqueName, ccps.get( 0 ).getLogicalColumnName(), partitionGroupId );
                        }
                    }
                }
            }
        } else {
            // Take the first column placement
            // Worst-case
            for ( long columnId : catalogTable.columnIds ) {
                relevantCcps.add( getPlacementsWithAllPartitionGroups( columnId, catalogTable.numPartitionGroups ).get( 0 ) );
            }
        }
        return relevantCcps;
    }


    @Override
    public boolean validatePartitionGroupSetup( List<List<String>> partitionGroupQualifiers, long numPartitionGroups, List<String> partitionGroupNames, CatalogColumn partitionColumn ) {
        super.validatePartitionGroupSetup( partitionGroupQualifiers, numPartitionGroups, partitionGroupNames, partitionColumn );

        if ( partitionColumn.type.getFamily() == PolyTypeFamily.NUMERIC ) {
            for ( List<String> singlePartitionQualifiers : partitionGroupQualifiers ) {
                for ( String qualifier : singlePartitionQualifiers ) {
                    try {
                        Integer.valueOf( qualifier );
                    } catch ( NumberFormatException e ) {
                        throw new RuntimeException( "Specified partition value '" + qualifier + "' is not a number as expected according to the type of the partition column " + partitionColumn.name );
                    }
                }
            }
        }

        if ( partitionGroupQualifiers.isEmpty() ) {
            throw new RuntimeException( "LIST Partitioning doesn't support  empty Partition Qualifiers: '" + partitionGroupQualifiers +
                    "'. USE (PARTITION name1 VALUES(value1)[(,PARTITION name1 VALUES(value1))*])" );
        }

        if ( partitionGroupQualifiers.size() + 1 != numPartitionGroups ) {
            throw new RuntimeException( "Number of partitionQualifiers '" + partitionGroupQualifiers +
                    "' + (mandatory 'Unbound' partition) is not equal to number of specified partitions '" + numPartitionGroups + "'" );
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
                .sqlPrefix( "PARTITION" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "" )
                .build() );

        dynamicRows.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( true )
                .modifiable( true )
                .sqlPrefix( "VALUES(" )
                .sqlSuffix( ")" )
                .valueSeparation( "," )
                .defaultValue( "" )
                .build() );

        //Fixed rows to display after dynamically generated ones
        List<List<PartitionFunctionInfoColumn>> rowsAfter = new ArrayList<>();
        List<PartitionFunctionInfoColumn> unboundRow = new ArrayList<>();
        unboundRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.LABEL )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "UNBOUND" )
                .build() );

        unboundRow.add( PartitionFunctionInfoColumn.builder()
                .fieldType( PartitionFunctionInfoColumnType.STRING )
                .mandatory( false )
                .modifiable( false )
                .sqlPrefix( "" )
                .sqlSuffix( "" )
                .valueSeparation( "" )
                .defaultValue( "automatically filled" )
                .build() );

        rowsAfter.add( unboundRow );

        PartitionFunctionInfo uiObject = PartitionFunctionInfo.builder()
                .functionTitle( FUNCTION_TITLE )
                .description( "Partitions data based on a comma-separated list of values which is assigned to a specific partition. "
                        + "Please surround string values with single quotes (e.g. 'foo'). Multiple values are separated by comma. "
                        + "INFO: Note that this partition function provides an 'UNBOUND' partition capturing all values that are not explicitly specified." )
                .sqlPrefix( "(" )
                .sqlSuffix( ")" )
                .rowSeparation( "," )
                .dynamicRows( dynamicRows )
                .headings( new ArrayList<>( Arrays.asList( "Partition Name", "Values" ) ) )
                .rowsAfter( rowsAfter )
                .build();

        return uiObject;
    }


    @Override
    public boolean requiresUnboundPartitionGroup() {
        return REQUIRES_UNBOUND_PARTITION_GROUP;
    }


    @Override
    public boolean supportsColumnOfType( PolyType type ) {
        return SUPPORTED_TYPES.contains( type );
    }

}

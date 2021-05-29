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

package org.polypheny.db.adapter.mongodb.rules;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.gridfs.GridFSBucket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.adapter.mongodb.MongoStore;
import org.polypheny.db.adapter.mongodb.MongoTable;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.adapter.mongodb.rules.MongoRules.MongoValues;
import org.polypheny.db.adapter.mongodb.util.MongoTypeUtil;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelRecordType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

class MongoTableModify extends TableModify implements MongoRel {


    protected MongoTableModify( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, CatalogReader catalogReader, RelNode input, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        super( cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new MongoTableModify(
                getCluster(),
                traitSet,
                getTable(),
                getCatalogReader(),
                AbstractRelNode.sole( inputs ),
                getOperation(),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened() );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.setDML( true );
        Table preTable = ((RelOptTableImpl) table).getTable();

        if ( !(preTable instanceof MongoTable) ) {
            throw new RuntimeException( "There seems to be a problem with the correct costs for one of stores." );
        }
        implementor.mongoTable = (MongoTable) preTable;
        implementor.table = table;
        implementor.setOperation( this.getOperation() );

        switch ( this.getOperation() ) {
            case INSERT: {
                if ( input instanceof MongoValues ) {
                    handleDirectInsert( implementor, ((MongoValues) input) );
                } else if ( input instanceof MongoProject ) {
                    handlePreparedInsert( implementor, ((MongoProject) input) );
                } else {
                    return;
                }
            }
            break;
            case UPDATE:
                Implementor condImplementor = new Implementor( true );
                condImplementor.setStaticRowType( implementor.getStaticRowType() );
                ((MongoRel) input).implement( condImplementor );
                implementor.filter = condImplementor.filter;

                MongoRowType rowType = condImplementor.getStaticRowType();
                int pos = 0;
                BsonDocument doc = new BsonDocument();
                GridFSBucket bucket = implementor.mongoTable.getMongoSchema().getBucket();
                for ( RexNode el : getSourceExpressionList() ) {
                    if ( el instanceof RexLiteral ) {
                        doc.append( rowType.getPhysicalName( getUpdateColumnList().get( pos ) ), MongoTypeUtil.getAsBson( (RexLiteral) el, bucket ) );
                    } else if ( el instanceof RexCall ) {
                        if ( ((RexCall) el).op.kind == SqlKind.PLUS ) {
                            doc.append( rowType.getPhysicalName( getUpdateColumnList().get( pos ) ), visitCall( implementor, (RexCall) el, SqlKind.PLUS, el.getType().getPolyType() ) );
                        } else {
                            doc.append( rowType.getPhysicalName( getUpdateColumnList().get( pos ) ), MongoTypeUtil.getBsonArray( (RexCall) el, bucket ) );
                        }
                    } else if ( el instanceof RexDynamicParam ) {
                        doc.append( rowType.getPhysicalName( getUpdateColumnList().get( pos ) ), new BsonDynamic( (RexDynamicParam) el ) );
                    }
                    pos++;
                }
                BsonDocument update = new BsonDocument().append( "$set", doc );

                implementor.operations = Collections.singletonList( update );

                break;
            case MERGE:
                break;
            case DELETE: {
                Implementor filterCollector = new Implementor( true );
                filterCollector.setStaticRowType( implementor.getStaticRowType() );
                ((MongoRel) input).implement( filterCollector );
                List<String> docs = new ArrayList<>();
                for ( Pair<String, String> el : filterCollector.list ) {
                    docs.add( el.right );
                }
                String docString = "";
                if ( docs.size() == 1 ) {
                    docString = docs.get( 0 );
                } else {
                    // TODO DL: evaluate if this is even possible
                }
                implementor.filter = filterCollector.filter;
            }

        }

    }


    private BsonValue visitCall( Implementor implementor, RexCall call, SqlKind op, PolyType type ) {
        BsonDocument doc = new BsonDocument();

        BsonArray array = new BsonArray();
        for ( RexNode operand : call.operands ) {
            if ( operand.getKind() == SqlKind.FIELD_ACCESS ) {
                String physicalName = "$" + implementor.getPhysicalName( ((RexFieldAccess) operand).getField().getName() );
                array.add( new BsonString( physicalName ) );
            } else if ( operand instanceof RexCall ) {
                array.add( visitCall( implementor, (RexCall) operand, ((RexCall) operand).op.getKind(), type ) );
            } else if ( operand.getKind() == SqlKind.LITERAL ) {
                array.add( MongoTypeUtil.getAsBson( ((RexLiteral) operand).getValueAs( MongoTypeUtil.getClassFromType( type ) ), type, implementor.mongoTable.getMongoSchema().getBucket() ) );
            } else if ( operand.getKind() == SqlKind.DYNAMIC_PARAM ) {
                array.add( new BsonDynamic( (RexDynamicParam) operand ) );
            } else {
                throw new RuntimeException( "Not implemented yet" );
            }
        }
        if ( op == SqlKind.PLUS ) {
            doc.append( "$add", array );
        } else if ( op == SqlKind.MINUS ) {
            doc.append( "$subtract", array );
        } else {
            throw new RuntimeException( "Not implemented yet" );
        }

        return doc;
    }


    private void handlePreparedInsert( Implementor implementor, MongoProject input ) {
        if ( !(input.getInput() instanceof MongoValues) && input.getInput().getRowType().getFieldList().size() == 1 ) {
            return;
        }
        // TODO DL: REFACTOR
        MongoValues values = (MongoValues) input.getInput();
        if ( values.tuples.size() > 0
                && values.getRowType().getFieldList().size() != 1
                && values.getRowType().getFieldList().get( 0 ).getName().equals( "ZERO" ) ) {
            // we have a partitioned table
            handleDirectInsert( implementor, values );
            return;
        }

        BsonDocument doc = new BsonDocument();
        CatalogTable catalogTable = implementor.mongoTable.getCatalogTable();
        GridFSBucket bucket = implementor.mongoTable.getMongoSchema().getBucket();
        Map<Integer, String> physicalMapping = getPhysicalMap( input.getRowType().getFieldList(), catalogTable );

        implementor.setStaticRowType( (RelRecordType) input.getRowType() );

        int pos = 0;
        for ( RexNode rexNode : input.getChildExps() ) {
            if ( rexNode instanceof RexDynamicParam ) {
                // preparedInsert
                doc.append( physicalMapping.get( pos ), new BsonDynamic( (RexDynamicParam) rexNode ) );

            } else if ( rexNode instanceof RexLiteral ) {
                doc.append( getPhysicalName( input, catalogTable, pos ), MongoTypeUtil.getAsBson( (RexLiteral) rexNode, bucket ) );
            } else if ( rexNode instanceof RexCall ) {
                PolyType type = ((RelOptTableImpl) table)
                        .getTable()
                        .getRowType( getCluster().getTypeFactory() )
                        .getFieldList()
                        .get( pos )
                        .getType()
                        .getComponentType()
                        .getPolyType();

                doc.append( physicalMapping.get( pos ), getBsonArray( (RexCall) rexNode, type, bucket ) );

            } else if ( rexNode.getKind() == SqlKind.INPUT_REF && input.getInput() instanceof MongoValues ) {
                // TODO DL handle and refactor
                handleDirectInsert( implementor, (MongoValues) input.getInput() );
                return;
            } else {
                throw new RuntimeException( "This rexType was not considered" );
            }

            pos++;
        }
        // we need to use the extended json format here to not loose precision like long -> int etc.
        implementor.operations = Collections.singletonList( doc );

    }


    private Map<Integer, String> getPhysicalMap( List<RelDataTypeField> fieldList, CatalogTable catalogTable ) {
        Map<Integer, String> map = new HashMap<>();
        List<String> names = catalogTable.getColumnNames();
        List<Long> ids = catalogTable.columnIds;
        int pos = 0;
        for ( String name : Pair.left( fieldList ) ) {
            map.put( pos, MongoStore.getPhysicalColumnName( ids.get( names.indexOf( name ) ) ) );
            pos++;
        }
        return map;
    }


    private String getPhysicalName( MongoProject input, CatalogTable catalogTable, int pos ) {
        String logicalName = input.getRowType().getFieldNames().get( pos );
        int index = catalogTable.getColumnNames().indexOf( logicalName );
        return MongoStore.getPhysicalColumnName( catalogTable.columnIds.get( index ) );
    }


    private BsonValue getBsonArray( RexCall el, PolyType type, GridFSBucket bucket ) {
        if ( el.op.kind == SqlKind.ARRAY_VALUE_CONSTRUCTOR ) {
            BsonArray array = new BsonArray();
            array.addAll( el.operands.stream().map( operand -> {
                if ( operand instanceof RexLiteral ) {
                    return MongoTypeUtil.getAsBson( MongoTypeUtil.getMongoComparable( type, (RexLiteral) operand ), type, bucket );
                } else if ( operand instanceof RexCall ) {
                    return getBsonArray( (RexCall) operand, type, bucket );
                }
                throw new RuntimeException( "The given RexCall could not be transformed correctly." );
            } ).collect( Collectors.toList() ) );
            return array;
        }
        throw new RuntimeException( "The given RexCall could not be transformed correctly." );
    }


    private void handleDirectInsert( Implementor implementor, MongoValues values ) {
        List<BsonDocument> docs = new ArrayList<>();
        CatalogTable catalogTable = implementor.mongoTable.getCatalogTable();
        GridFSBucket bucket = implementor.mongoTable.getMongoSchema().getBucket();

        for ( ImmutableList<RexLiteral> literals : values.tuples ) {
            BsonDocument doc = new BsonDocument();
            int pos = 0;
            for ( RexLiteral literal : literals ) {
                doc.append( MongoStore.getPhysicalColumnName( catalogTable.columnIds.get( pos ) ), MongoTypeUtil.getAsBson( literal, bucket ) );
                pos++;
            }
            docs.add( doc );
        }
        implementor.operations = docs;
    }

}

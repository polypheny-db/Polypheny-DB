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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.util.JsonBuilder;
import org.polypheny.db.util.Pair;


/**
 * Implementation of a {@link org.polypheny.db.rel.core.Filter}
 * relational expression in MongoDB.
 */
public class MongoFilter extends Filter implements MongoRel {

    public MongoFilter( RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition ) {
        super( cluster, traitSet, child, condition );
        assert getConvention() == CONVENTION;
        assert getConvention() == child.getConvention();
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public MongoFilter copy( RelTraitSet traitSet, RelNode input, RexNode condition ) {
        return new MongoFilter( getCluster(), traitSet, input, condition );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        // to not break the existing functionality for now we have to handle it this way
        Translator translator = null;
        if ( implementor.getStaticRowType() != null && implementor.getStaticRowType() instanceof MongoRowType ) {
            translator = new Translator( MongoRules.mongoFieldNames( getRowType() ), (MongoRowType) implementor.getStaticRowType() );
        } else {
            translator = new Translator( MongoRules.mongoFieldNames( getRowType() ) );
        }
        String match = translator.translateMatch( condition, implementor.isDML() );

        if ( translator.dynamics.size() > 0 ) {
            // we merge the dynamic BsonDocument with the static conditions
            implementor.dynamicConditions = new BsonDocument().append( "$or", translator.dynamics );
            if ( !implementor.isDML() ) {
                implementor.dynamicConditions = new BsonDocument().append( "$match", implementor.dynamicConditions );
            }
            implementor.filter = implementor.dynamicConditions.toJson();
            match = implementor.filter;
        }
        implementor.add( null, match );
    }


    /**
     * Translates {@link RexNode} expressions into MongoDB expression strings.
     */
    static class Translator {

        final JsonBuilder builder = new JsonBuilder();
        final Multimap<String, Pair<String, RexLiteral>> multimap = HashMultimap.create();
        final Map<String, RexLiteral> eqMap = new LinkedHashMap<>();
        private final List<String> fieldNames;
        private final MongoRowType rowType;
        private final Map<String, List<RexNode>> arrayMap = new HashMap<>();
        private final BsonArray dynamics = new BsonArray();


        Translator( List<String> fieldNames ) {
            this( fieldNames, null );
        }


        Translator( List<String> fieldNames, MongoRowType rowType ) {
            this.builder.setMongo( true );
            this.fieldNames = fieldNames;
            this.rowType = rowType;
        }


        private String translateMatch( RexNode condition, boolean isDDL ) {
            if ( !isDDL ) {
                Map<String, Object> map = builder.map();
                map.put( "$match", translateOr( condition ) );
                return builder.toJsonString( map );
            } else {
                // TODO DL: kinda hacky maybe need to implement cleaner in the future
                return builder.toJsonString( translateOr( condition ) );
            }
        }


        private Object translateOr( RexNode condition ) {
            List<Object> list = new ArrayList<>();
            for ( RexNode node : RelOptUtil.disjunctions( condition ) ) {
                list.add( translateAnd( node ) );
            }
            switch ( list.size() ) {
                case 1:
                    return list.get( 0 );
                default:
                    Map<String, Object> map = builder.map();
                    map.put( "$or", list );
                    return map;
            }
        }


        /**
         * Translates a condition that may be an AND of other conditions. Gathers together conditions that apply to the same field.
         */
        private Map<String, Object> translateAnd( RexNode node0 ) {
            eqMap.clear();
            multimap.clear();
            for ( RexNode node : RelOptUtil.conjunctions( node0 ) ) {
                translateMatch2( node );
            }
            Map<String, Object> map = builder.map();
            for ( Map.Entry<String, RexLiteral> entry : eqMap.entrySet() ) {
                multimap.removeAll( entry.getKey() );
                map.put( entry.getKey(), literalValue( entry.getValue() ) );
            }
            for ( Map.Entry<String, Collection<Pair<String, RexLiteral>>> entry : multimap.asMap().entrySet() ) {
                Map<String, Object> map2 = builder.map();
                for ( Pair<String, RexLiteral> s : entry.getValue() ) {
                    addPredicate( map2, s.left, literalValue( s.right ) );
                }
                map.put( entry.getKey(), map2 );
            }
            for ( Entry<String, List<RexNode>> entry : arrayMap.entrySet() ) {
                map.put( entry.getKey(), entry.getValue().stream().map( el -> literalValue( (RexLiteral) el ) ).collect( Collectors.toList() ) );
            }

            return map;
        }


        private void addPredicate( Map<String, Object> map, String op, Object v ) {
            if ( map.containsKey( op ) && stronger( op, map.get( op ), v ) ) {
                return;
            }
            map.put( op, v );
        }


        /**
         * Returns whether {@code v0} is a stronger value for operator {@code key} than {@code v1}.
         *
         * For example, {@code stronger("$lt", 100, 200)} returns true, because "&lt; 100" is a more powerful condition than "&lt; 200".
         */
        private boolean stronger( String key, Object v0, Object v1 ) {
            if ( key.equals( "$lt" ) || key.equals( "$lte" ) ) {
                if ( v0 instanceof Number && v1 instanceof Number ) {
                    return ((Number) v0).doubleValue() < ((Number) v1).doubleValue();
                }
                if ( v0 instanceof String && v1 instanceof String ) {
                    return v0.toString().compareTo( v1.toString() ) < 0;
                }
            }
            if ( key.equals( "$gt" ) || key.equals( "$gte" ) ) {
                return stronger( "$lt", v1, v0 );
            }
            return false;
        }


        private static Object literalValue( RexLiteral literal ) {
            return literal.getValue3();
        }


        private Void translateMatch2( RexNode node ) {
            switch ( node.getKind() ) {
                case EQUALS:
                    return translateBinary( null, null, (RexCall) node );
                case LESS_THAN:
                    return translateBinary( "$lt", "$gt", (RexCall) node );
                case LESS_THAN_OR_EQUAL:
                    return translateBinary( "$lte", "$gte", (RexCall) node );
                case NOT_EQUALS:
                    return translateBinary( "$ne", "$ne", (RexCall) node );
                case GREATER_THAN:
                    return translateBinary( "$gt", "$lt", (RexCall) node );
                case GREATER_THAN_OR_EQUAL:
                    return translateBinary( "$gte", "$lte", (RexCall) node );
                default:
                    throw new AssertionError( "cannot translate " + node );
            }
        }


        /**
         * Translates a call to a binary operator, reversing arguments if necessary.
         */
        private Void translateBinary( String op, String rop, RexCall call ) {
            final RexNode left = call.operands.get( 0 );
            final RexNode right = call.operands.get( 1 );
            boolean b = translateBinary2( op, left, right );
            if ( b ) {
                return null;
            }
            b = translateBinary2( rop, right, left );
            if ( b ) {
                return null;
            }
            b = translateArray( op, right, left );
            if ( b ) {
                return null;
            }
            b = translateArray( op, left, right );
            if ( b ) {
                return null;
            }
            throw new AssertionError( "cannot translate op " + op + " call " + call );
        }


        private boolean translateArray( String op, RexNode right, RexNode left ) {
            if ( right instanceof RexCall && left instanceof RexInputRef ) {
                // $9 ( index ) -> [el1, el2]
                String name = getPhysicalName( (RexInputRef) left );
                arrayMap.put( name, ((RexCall) right).operands );

                return true;
            } else if ( right instanceof RexCall && left instanceof RexLiteral ) {
                // $9[1] -> el1
                String name = getPhysicalName( (RexInputRef) ((RexCall) right).operands.get( 0 ) );
                // we have to adjust as mongodb arrays start at 0 and sql at 1
                int pos = ((RexLiteral) ((RexCall) right).operands.get( 1 )).getValueAs( Integer.class ) - 1;

                eqMap.put( name + "." + pos, (RexLiteral) left );

                return true;
            }
            return false;
        }


        /**
         * Translates a call to a binary operator. Returns whether successful.
         */
        private boolean translateBinary2( String op, RexNode left, RexNode right ) {
            switch ( right.getKind() ) {
                case LITERAL:
                    break;
                case DYNAMIC_PARAM:
                    attachDynamic( left, (RexDynamicParam) right, op );
                    return true;
                default:
                    return false;
            }
            final RexLiteral rightLiteral = (RexLiteral) right;
            switch ( left.getKind() ) {
                case INPUT_REF:
                    translateOp2( op, getPhysicalName( (RexInputRef) left ), rightLiteral );
                    return true;
                case CAST:
                    return translateBinary2( op, ((RexCall) left).operands.get( 0 ), right );
                case OTHER_FUNCTION:

                    String itemName = MongoRules.isItem( (RexCall) left );
                    if ( itemName != null ) {
                        translateOp2( op, itemName, rightLiteral );
                        return true;
                    }
                    // fall through
                default:
                    return false;
            }
        }


        private void attachDynamic( RexNode left, RexDynamicParam right, String op ) {
            if ( left.getKind() == SqlKind.INPUT_REF ) {
                if ( op == null ) {
                    this.dynamics
                            .add(
                                    new BsonDocument()
                                            .append(
                                                    getPhysicalName( (RexInputRef) left ),
                                                    new BsonDynamic( right ) ) );
                } else {
                    this.dynamics
                            .add(
                                    new BsonDocument()
                                            .append(
                                                    getPhysicalName( (RexInputRef) left ),
                                                    new BsonDocument().append( op, new BsonDynamic( right ) ) ) );
                }
                //this.dynamics.add( new MongoPair<>( getPhysicalName( (RexInputRef) left ), right.getIndex(), op ) );
            }
        }


        private String getPhysicalName( RexInputRef left ) {
            final RexInputRef left1 = left;
            String name = fieldNames.get( left1.getIndex() );
            if ( rowType != null && rowType.getId( name ) != null ) {
                name = rowType.getPhysicalName( name );
            }
            return name;
        }


        private void translateOp2( String op, String name, RexLiteral right ) {
            if ( op == null ) {
                // E.g.: {deptno: 100}
                eqMap.put( name, right );
                dynamics.add( new BsonDocument().append( name, MongoTypeUtil.getAsBson( right, null ) ) );
            } else {
                // E.g. {deptno: {$lt: 100}} which may later be combined with other conditions: E.g. {deptno: [$lt: 100, $gt: 50]}
                multimap.put( name, Pair.of( op, right ) );
                dynamics.add( new BsonDocument().append( name, new BsonDocument().append( op, MongoTypeUtil.getAsBson( right, null ) ) ) );
            }
        }

    }

}


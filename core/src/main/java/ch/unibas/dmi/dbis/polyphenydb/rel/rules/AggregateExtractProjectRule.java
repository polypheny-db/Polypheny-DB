/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.AggregateCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet.Builder;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mapping;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.MappingType;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;


/**
 * Rule to extract a {@link Project} from an {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate} and push it down towards the input.
 *
 * What projections can be safely pushed down depends upon which fields the Aggregate uses.
 *
 * To prevent cycles, this rule will not extract a {@code Project} if the {@code Aggregate}s input is already a {@code Project}.
 */
public class AggregateExtractProjectRule extends RelOptRule {

    /**
     * Creates an AggregateExtractProjectRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public AggregateExtractProjectRule( Class<? extends Aggregate> aggregateClass, Class<? extends RelNode> inputClass, RelBuilderFactory relBuilderFactory ) {
        // Predicate prevents matching against an Aggregate whose input is already a Project. Prevents this rule firing repeatedly.
        this( operand( aggregateClass, operandJ( inputClass, null, r -> !(r instanceof Project), any() ) ), relBuilderFactory );
    }


    public AggregateExtractProjectRule( RelOptRuleOperand operand, RelBuilderFactory builderFactory ) {
        super( operand, builderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Aggregate aggregate = call.rel( 0 );
        final RelNode input = call.rel( 1 );
        // Compute which input fields are used.
        // 1. group fields are always used
        final Builder inputFieldsUsed = aggregate.getGroupSet().rebuild();
        // 2. agg functions
        for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
            for ( int i : aggCall.getArgList() ) {
                inputFieldsUsed.set( i );
            }
            if ( aggCall.filterArg >= 0 ) {
                inputFieldsUsed.set( aggCall.filterArg );
            }
        }
        final RelBuilder relBuilder = call.builder().push( input );
        final List<RexNode> projects = new ArrayList<>();
        final Mapping mapping = Mappings.create( MappingType.INVERSE_SURJECTION, aggregate.getInput().getRowType().getFieldCount(), inputFieldsUsed.cardinality() );
        int j = 0;
        for ( int i : inputFieldsUsed.build() ) {
            projects.add( relBuilder.field( i ) );
            mapping.set( i, j++ );
        }

        relBuilder.project( projects );

        final ImmutableBitSet newGroupSet = Mappings.apply( mapping, aggregate.getGroupSet() );

        final Iterable<ImmutableBitSet> newGroupSets = Iterables.transform( aggregate.getGroupSets(), bitSet -> Mappings.apply( mapping, bitSet ) );
        final List<RelBuilder.AggCall> newAggCallList = new ArrayList<>();
        for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
            final ImmutableList<RexNode> args = relBuilder.fields( Mappings.apply2( mapping, aggCall.getArgList() ) );
            final RexNode filterArg = aggCall.filterArg < 0 ? null : relBuilder.field( Mappings.apply( mapping, aggCall.filterArg ) );
            newAggCallList.add(
                    relBuilder.aggregateCall( aggCall.getAggregation(), args )
                            .distinct( aggCall.isDistinct() )
                            .filter( filterArg )
                            .approximate( aggCall.isApproximate() )
                            .sort( relBuilder.fields( aggCall.collation ) )
                            .as( aggCall.name ) );
        }

        final RelBuilder.GroupKey groupKey = relBuilder.groupKey( newGroupSet, newGroupSets );
        relBuilder.aggregate( groupKey, newAggCallList );
        call.transformTo( relBuilder.build() );
    }
}

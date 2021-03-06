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

package org.polypheny.db.plan.volcano;


import static org.junit.Assert.assertTrue;
import static org.polypheny.db.plan.volcano.PlannerTests.PHYS_CALLING_CONVENTION;
import static org.polypheny.db.plan.volcano.PlannerTests.TestLeafRel;
import static org.polypheny.db.plan.volcano.PlannerTests.TestSingleRel;
import static org.polypheny.db.plan.volcano.PlannerTests.newCluster;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelTraitDef;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollationImpl;
import org.polypheny.db.rel.RelFieldCollation;
import org.polypheny.db.rel.RelFieldCollation.Direction;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rex.RexNode;


/**
 * Unit test for {@link org.polypheny.db.rel.RelCollationTraitDef}.
 */
public class CollationConversionTest {

    private static final TestRelCollationImpl LEAF_COLLATION = new TestRelCollationImpl( ImmutableList.of( new RelFieldCollation( 0, Direction.CLUSTERED ) ) );

    private static final TestRelCollationImpl ROOT_COLLATION = new TestRelCollationImpl( ImmutableList.of( new RelFieldCollation( 0 ) ) );

    private static final TestRelCollationTraitDef COLLATION_TRAIT_DEF = new TestRelCollationTraitDef();


    @Test
    public void testCollationConversion() {
        final VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef( ConventionTraitDef.INSTANCE );
        planner.addRelTraitDef( COLLATION_TRAIT_DEF );

        planner.addRule( new SingleNodeRule() );
        planner.addRule( new LeafTraitRule() );
        planner.addRule( ExpandConversionRule.INSTANCE );

        final RelOptCluster cluster = newCluster( planner );
        final NoneLeafRel leafRel = new NoneLeafRel( cluster, "a" );
        final NoneSingleRel singleRel = new NoneSingleRel( cluster, leafRel );
        final RelNode convertedRel = planner.changeTraits( singleRel, cluster.traitSetOf( PHYS_CALLING_CONVENTION ).plus( ROOT_COLLATION ) );
        planner.setRoot( convertedRel );
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue( result instanceof RootSingleRel );
        assertTrue( result.getTraitSet().contains( ROOT_COLLATION ) );
        assertTrue( result.getTraitSet().contains( PHYS_CALLING_CONVENTION ) );

        final RelNode input = result.getInput( 0 );
        assertTrue( input instanceof PhysicalSort );
        assertTrue( result.getTraitSet().contains( ROOT_COLLATION ) );
        assertTrue( input.getTraitSet().contains( PHYS_CALLING_CONVENTION ) );

        final RelNode input2 = input.getInput( 0 );
        assertTrue( input2 instanceof LeafRel );
        assertTrue( input2.getTraitSet().contains( LEAF_COLLATION ) );
        assertTrue( input.getTraitSet().contains( PHYS_CALLING_CONVENTION ) );
    }


    /**
     * Converts a NoneSingleRel to RootSingleRel.
     */
    private static class SingleNodeRule extends RelOptRule {

        SingleNodeRule() {
            super( operand( NoneSingleRel.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            NoneSingleRel single = call.rel( 0 );
            RelNode input = single.getInput();
            RelNode physInput = convert( input,
                    single.getTraitSet()
                            .replace( PHYS_CALLING_CONVENTION )
                            .plus( ROOT_COLLATION ) );
            call.transformTo( new RootSingleRel( single.getCluster(), physInput ) );
        }
    }


    /**
     * Root node with physical convention and ROOT_COLLATION trait.
     */
    private static class RootSingleRel extends TestSingleRel {

        RootSingleRel( RelOptCluster cluster, RelNode input ) {
            super( cluster, cluster.traitSetOf( PHYS_CALLING_CONVENTION ).plus( ROOT_COLLATION ), input );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            return new RootSingleRel( getCluster(), sole( inputs ) );
        }
    }


    /**
     * Converts a {@link NoneLeafRel} (with none convention) to {@link LeafRel} (with physical convention).
     */
    private static class LeafTraitRule extends RelOptRule {

        LeafTraitRule() {
            super( operand( NoneLeafRel.class, any() ) );
        }


        @Override
        public Convention getOutConvention() {
            return PHYS_CALLING_CONVENTION;
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            NoneLeafRel leafRel = call.rel( 0 );
            call.transformTo( new LeafRel( leafRel.getCluster(), leafRel.label ) );
        }
    }


    /**
     * Leaf node with physical convention and LEAF_COLLATION trait.
     */
    private static class LeafRel extends TestLeafRel {

        LeafRel( RelOptCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( PHYS_CALLING_CONVENTION ).plus( LEAF_COLLATION ), label );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            return new LeafRel( getCluster(), label );
        }
    }


    /**
     * Leaf node with none convention and LEAF_COLLATION trait.
     */
    private static class NoneLeafRel extends TestLeafRel {

        NoneLeafRel( RelOptCluster cluster, String label ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ).plus( LEAF_COLLATION ), label );
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            assert traitSet.comprises( Convention.NONE, LEAF_COLLATION );
            assert inputs.isEmpty();
            return this;
        }
    }


    /**
     * A single-input node with none convention and LEAF_COLLATION trait.
     */
    private static class NoneSingleRel extends TestSingleRel {

        NoneSingleRel( RelOptCluster cluster, RelNode input ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ).plus( LEAF_COLLATION ), input );
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            assert traitSet.comprises( Convention.NONE, LEAF_COLLATION );
            return new NoneSingleRel( getCluster(), sole( inputs ) );
        }
    }


    /**
     * Dummy collation trait implementation for the test.
     */
    private static class TestRelCollationImpl extends RelCollationImpl {

        TestRelCollationImpl( ImmutableList<RelFieldCollation> fieldCollations ) {
            super( fieldCollations );
        }


        @Override
        public RelTraitDef getTraitDef() {
            return COLLATION_TRAIT_DEF;
        }
    }


    /**
     * Dummy collation trait def implementation for the test (uses {@link PhysicalSort} below).
     */
    private static class TestRelCollationTraitDef extends RelTraitDef<RelCollation> {

        @Override
        public Class<RelCollation> getTraitClass() {
            return RelCollation.class;
        }


        @Override
        public String getSimpleName() {
            return "testsort";
        }


        @Override
        public boolean multiple() {
            return true;
        }


        @Override
        public RelCollation getDefault() {
            return LEAF_COLLATION;
        }


        @Override
        public RelNode convert( RelOptPlanner planner, RelNode rel, RelCollation toCollation, boolean allowInfiniteCostConverters ) {
            if ( toCollation.getFieldCollations().isEmpty() ) {
                // An empty sort doesn't make sense.
                return null;
            }

            return new PhysicalSort( rel.getCluster(), rel.getTraitSet().replace( toCollation ), rel, toCollation, null, null );
        }


        @Override
        public boolean canConvert( RelOptPlanner planner, RelCollation fromTrait, RelCollation toTrait ) {
            return true;
        }
    }


    /**
     * Physical sort node (not logical).
     */
    private static class PhysicalSort extends Sort {

        PhysicalSort( RelOptCluster cluster, RelTraitSet traits, RelNode input, RelCollation collation, RexNode offset, RexNode fetch ) {
            super( cluster, traits, input, collation, offset, fetch );
        }


        @Override
        public Sort copy( RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch ) {
            return new PhysicalSort( getCluster(), traitSet, newInput, newCollation, offset, fetch );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return planner.getCostFactory().makeTinyCost();
        }
    }
}


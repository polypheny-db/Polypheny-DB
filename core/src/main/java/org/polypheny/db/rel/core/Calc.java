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

package ch.unibas.dmi.dbis.polyphenydb.rel.core;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.SingleRel;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMdUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLocalRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexShuttle;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import java.util.List;


/**
 * <code>Calc</code> is an abstract base class for implementations of {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalCalc}.
 */
public abstract class Calc extends SingleRel {

    protected final RexProgram program;


    /**
     * Creates a Calc.
     *
     * @param cluster Cluster
     * @param traits Traits
     * @param child Input relation
     * @param program Calc program
     */
    protected Calc( RelOptCluster cluster, RelTraitSet traits, RelNode child, RexProgram program ) {
        super( cluster, traits, child );
        this.rowType = program.getOutputRowType();
        this.program = program;
        assert isValid( Litmus.THROW, null );
    }


    @Override
    public final Calc copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return copy( traitSet, sole( inputs ), program );
    }


    /**
     * Creates a copy of this {@code Calc}.
     *
     * @param traitSet Traits
     * @param child Input relation
     * @param program Calc program
     * @return New {@code Calc} if any parameter differs from the value of this {@code Calc}, or just {@code this} if all the parameters are the same
     * @see #copy(ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet, java.util.List)
     */
    public abstract Calc copy( RelTraitSet traitSet, RelNode child, RexProgram program );


    @Override
    public boolean isValid( Litmus litmus, Context context ) {
        if ( !RelOptUtil.equal(
                "program's input type",
                program.getInputRowType(),
                "child's output type",
                getInput().getRowType(),
                litmus ) ) {
            return litmus.fail( null );
        }
        if ( !program.isValid( litmus, context ) ) {
            return litmus.fail( null );
        }
        if ( !program.isNormalized( litmus, getCluster().getRexBuilder() ) ) {
            return litmus.fail( null );
        }
        return litmus.succeed();
    }


    public RexProgram getProgram() {
        return program;
    }


    @Override
    public double estimateRowCount( RelMetadataQuery mq ) {
        return RelMdUtil.estimateFilteredRows( getInput(), program, mq );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        double dRows = mq.getRowCount( this );
        double dCpu = mq.getRowCount( getInput() ) * program.getExprCount();
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        return program.explainCalc( super.explainTerms( pw ) );
    }


    @Override
    public RelNode accept( RexShuttle shuttle ) {
        List<RexNode> oldExprs = program.getExprList();
        List<RexNode> exprs = shuttle.apply( oldExprs );
        List<RexLocalRef> oldProjects = program.getProjectList();
        List<RexLocalRef> projects = shuttle.apply( oldProjects );
        RexLocalRef oldCondition = program.getCondition();
        RexNode condition;
        if ( oldCondition != null ) {
            condition = shuttle.apply( oldCondition );
            assert condition instanceof RexLocalRef : "Invalid condition after rewrite. Expected RexLocalRef, got " + condition;
        } else {
            condition = null;
        }
        if ( exprs == oldExprs && projects == oldProjects && condition == oldCondition ) {
            return this;
        }
        return copy( traitSet, getInput(), new RexProgram( program.getInputRowType(), exprs, projects, (RexLocalRef) condition, program.getOutputRowType() ) );
    }
}

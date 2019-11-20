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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * MultiJoinProjectTransposeRule implements the rule for pulling
 * {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject}s that are on top of a
 * {@link MultiJoin} and beneath a
 * {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin} so the
 * {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject} appears above the
 * {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin}.
 *
 * In the process of doing so, also save away information about the respective fields that are referenced in the expressions in the {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject} we're pulling up, as
 * well as the join condition, in the resultant {@link MultiJoin}s
 *
 * For example, if we have the following sub-query:
 *
 * <blockquote><pre>(select X.x1, Y.y1 from X, Y where X.x2 = Y.y2 and X.x3 = 1 and Y.y3 = 2)</pre></blockquote>
 *
 * The {@link MultiJoin} associated with (X, Y) associates x1 with X and y1 with Y. Although x3 and y3 need to be read due to the filters, they are not required after the row scan has completed and therefore are not saved.
 * The join fields, x2 and y2, are also tracked separately.
 *
 * Note that by only pulling up projects that are on top of {@link MultiJoin}s, we preserve projections on top of row scans.
 *
 * See the superclass for details on restrictions regarding which {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject}s cannot be pulled.
 */
public class MultiJoinProjectTransposeRule extends JoinProjectTransposeRule {

    public static final MultiJoinProjectTransposeRule MULTI_BOTH_PROJECT =
            new MultiJoinProjectTransposeRule(
                    operand(
                            LogicalJoin.class,
                            operand( LogicalProject.class, operand( MultiJoin.class, any() ) ),
                            operand( LogicalProject.class, operand( MultiJoin.class, any() ) ) ),
                    RelFactories.LOGICAL_BUILDER,
                    "MultiJoinProjectTransposeRule: with two LogicalProject children" );

    public static final MultiJoinProjectTransposeRule MULTI_LEFT_PROJECT =
            new MultiJoinProjectTransposeRule(
                    operand(
                            LogicalJoin.class,
                            some( operand( LogicalProject.class, operand( MultiJoin.class, any() ) ) ) ),
                    RelFactories.LOGICAL_BUILDER,
                    "MultiJoinProjectTransposeRule: with LogicalProject on left" );

    public static final MultiJoinProjectTransposeRule MULTI_RIGHT_PROJECT =
            new MultiJoinProjectTransposeRule(
                    operand(
                            LogicalJoin.class,
                            operand( RelNode.class, any() ),
                            operand( LogicalProject.class, operand( MultiJoin.class, any() ) ) ),
                    RelFactories.LOGICAL_BUILDER,
                    "MultiJoinProjectTransposeRule: with LogicalProject on right" );


    /**
     * Creates a MultiJoinProjectTransposeRule.
     */
    public MultiJoinProjectTransposeRule( RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description ) {
        super( operand, description, false, relBuilderFactory );
    }


    // override JoinProjectTransposeRule
    @Override
    protected boolean hasLeftChild( RelOptRuleCall call ) {
        return call.rels.length != 4;
    }


    // override JoinProjectTransposeRule
    @Override
    protected boolean hasRightChild( RelOptRuleCall call ) {
        return call.rels.length > 3;
    }


    // override JoinProjectTransposeRule
    @Override
    protected LogicalProject getRightChild( RelOptRuleCall call ) {
        if ( call.rels.length == 4 ) {
            return call.rel( 2 );
        } else {
            return call.rel( 3 );
        }
    }


    // override JoinProjectTransposeRule
    protected RelNode getProjectChild( RelOptRuleCall call, LogicalProject project, boolean leftChild ) {
        // Locate the appropriate MultiJoin based on which rule was fired and which projection we're dealing with
        MultiJoin multiJoin;
        if ( leftChild ) {
            multiJoin = call.rel( 2 );
        } else if ( call.rels.length == 4 ) {
            multiJoin = call.rel( 3 );
        } else {
            multiJoin = call.rel( 4 );
        }

        // Create a new MultiJoin that reflects the columns in the projection above the MultiJoin
        return RelOptUtil.projectMultiJoin( multiJoin, project );
    }
}


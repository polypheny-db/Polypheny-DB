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
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Union;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalUnion;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Planner rule that translates a distinct {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Union} (<code>all</code> = <code>false</code>) into an {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate}
 * on top of a non-distinct {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Union} (<code>all</code> = <code>true</code>).
 */
public class UnionToDistinctRule extends RelOptRule {

    public static final UnionToDistinctRule INSTANCE = new UnionToDistinctRule( LogicalUnion.class, RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a UnionToDistinctRule.
     */
    public UnionToDistinctRule( Class<? extends Union> unionClazz, RelBuilderFactory relBuilderFactory ) {
        super( operand( unionClazz, any() ), relBuilderFactory, null );
    }


    @Deprecated // to be removed before 2.0
    public UnionToDistinctRule( Class<? extends Union> unionClazz, RelFactories.SetOpFactory setOpFactory ) {
        this( unionClazz, RelBuilder.proto( setOpFactory ) );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Union union = call.rel( 0 );
        if ( union.all ) {
            return; // nothing to do
        }
        final RelBuilder relBuilder = call.builder();
        relBuilder.pushAll( union.getInputs() );
        relBuilder.union( true, union.getInputs().size() );
        relBuilder.distinct();
        call.transformTo( relBuilder.build() );
    }
}


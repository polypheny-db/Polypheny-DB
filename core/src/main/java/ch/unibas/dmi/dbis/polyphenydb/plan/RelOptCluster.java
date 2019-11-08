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

package ch.unibas.dmi.dbis.polyphenydb.plan;


import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.DefaultRelMetadataProvider;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.MetadataFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.MetadataFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataProvider;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * An environment for related relational expressions during the optimization of a query.
 */
public class RelOptCluster {

    private final RelDataTypeFactory typeFactory;
    private final RelOptPlanner planner;
    private final AtomicInteger nextCorrel;
    private final Map<String, RelNode> mapCorrelToRel;
    private RexNode originalExpression;
    private final RexBuilder rexBuilder;
    private RelMetadataProvider metadataProvider;
    private MetadataFactory metadataFactory;
    private final RelTraitSet emptyTraitSet;
    private RelMetadataQuery mq;


    /**
     * Creates a cluster.
     *
     * For use only from {@link #create} and {@link RelOptQuery}.
     */
    private RelOptCluster( RelOptPlanner planner, RelDataTypeFactory typeFactory, RexBuilder rexBuilder, AtomicInteger nextCorrel, Map<String, RelNode> mapCorrelToRel ) {
        this.nextCorrel = nextCorrel;
        this.mapCorrelToRel = mapCorrelToRel;
        this.planner = Objects.requireNonNull( planner );
        this.typeFactory = Objects.requireNonNull( typeFactory );
        this.rexBuilder = rexBuilder;
        this.originalExpression = rexBuilder.makeLiteral( "?" );

        // set up a default rel metadata provider, giving the planner first crack at everything
        setMetadataProvider( DefaultRelMetadataProvider.INSTANCE );
        this.emptyTraitSet = planner.emptyTraitSet();
        assert emptyTraitSet.size() == planner.getRelTraitDefs().size();
    }


    /**
     * Creates a cluster.
     */
    public static RelOptCluster create( RelOptPlanner planner, RexBuilder rexBuilder ) {
        return new RelOptCluster( planner, rexBuilder.getTypeFactory(), rexBuilder, new AtomicInteger( 0 ), new HashMap<>() );
    }


    public RelOptPlanner getPlanner() {
        return planner;
    }


    public RelDataTypeFactory getTypeFactory() {
        return typeFactory;
    }


    public RexBuilder getRexBuilder() {
        return rexBuilder;
    }


    public RelMetadataProvider getMetadataProvider() {
        return metadataProvider;
    }


    /**
     * Overrides the default metadata provider for this cluster.
     *
     * @param metadataProvider custom provider
     */
    public void setMetadataProvider( RelMetadataProvider metadataProvider ) {
        this.metadataProvider = metadataProvider;
        this.metadataFactory = new MetadataFactoryImpl( metadataProvider );
    }


    public MetadataFactory getMetadataFactory() {
        return metadataFactory;
    }


    /**
     * Returns the current RelMetadataQuery.
     *
     * This method might be changed or moved in future. If you have a {@link RelOptRuleCall} available, for example if you are in
     * a {@link RelOptRule#onMatch(RelOptRuleCall)} method, then use {@link RelOptRuleCall#getMetadataQuery()} instead.
     */
    public RelMetadataQuery getMetadataQuery() {
        if ( mq == null ) {
            mq = RelMetadataQuery.instance();
        }
        return mq;
    }


    /**
     * Should be called whenever the current {@link RelMetadataQuery} becomes invalid. Typically invoked from {@link RelOptRuleCall#transformTo}.
     */
    public void invalidateMetadataQuery() {
        mq = null;
    }


    /**
     * Constructs a new id for a correlating variable. It is unique within the whole query.
     */
    public CorrelationId createCorrel() {
        return new CorrelationId( nextCorrel.getAndIncrement() );
    }


    /**
     * Returns the default trait set for this cluster.
     */
    public RelTraitSet traitSet() {
        return emptyTraitSet;
    }


    public RelTraitSet traitSetOf( RelTrait trait ) {
        return emptyTraitSet.replace( trait );
    }
}


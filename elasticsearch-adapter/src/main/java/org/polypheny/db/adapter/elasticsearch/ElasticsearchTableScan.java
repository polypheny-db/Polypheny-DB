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

package org.polypheny.db.adapter.elasticsearch;


import java.util.List;
import java.util.Objects;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.polypheny.db.rel.type.RelDataType;


/**
 * Relational expression representing a scan of an Elasticsearch type.
 *
 * <p> Additional operations might be applied,
 * using the "find" method.</p>
 */
public class ElasticsearchTableScan extends TableScan implements ElasticsearchRel {

    private final ElasticsearchTable elasticsearchTable;
    private final RelDataType projectRowType;


    /**
     * Creates an ElasticsearchTableScan.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param table Table
     * @param elasticsearchTable Elasticsearch table
     * @param projectRowType Fields and types to project; null to project raw row
     */
    ElasticsearchTableScan( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, ElasticsearchTable elasticsearchTable, RelDataType projectRowType ) {
        super( cluster, traitSet, table );
        this.elasticsearchTable = Objects.requireNonNull( elasticsearchTable, "elasticsearchTable" );
        this.projectRowType = projectRowType;

        assert getConvention() == ElasticsearchRel.CONVENTION;
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        assert inputs.isEmpty();
        return this;
    }


    @Override
    public RelDataType deriveRowType() {
        return projectRowType != null ? projectRowType : super.deriveRowType();
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        final float f = projectRowType == null ? 1f : (float) projectRowType.getFieldCount() / 100f;
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 * f );
    }


    @Override
    public void register( RelOptPlanner planner ) {
        planner.addRule( ElasticsearchToEnumerableConverterRule.INSTANCE );
        for ( RelOptRule rule : ElasticsearchRules.RULES ) {
            planner.addRule( rule );
        }

        // remove this rule otherwise elastic can't correctly interpret approx_count_distinct() it is converted to cardinality aggregation in Elastic
        planner.removeRule( AggregateExpandDistinctAggregatesRule.INSTANCE );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.elasticsearchTable = elasticsearchTable;
        implementor.table = table;
    }
}


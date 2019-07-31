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


import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.materialize.Lattice;
import ch.unibas.dmi.dbis.polyphenydb.materialize.MaterializationService;
import ch.unibas.dmi.dbis.polyphenydb.materialize.TileKey;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import java.util.List;


/**
 * Use of a lattice by the query optimizer.
 */
public class RelOptLattice {

    public final Lattice lattice;
    public final RelOptTable starRelOptTable;


    public RelOptLattice( Lattice lattice, RelOptTable starRelOptTable ) {
        this.lattice = lattice;
        this.starRelOptTable = starRelOptTable;
    }


    public RelOptTable rootTable() {
        return lattice.rootNode.relOptTable();
    }


    /**
     * Rewrites a relational expression to use a lattice.
     *
     * Returns null if a rewrite is not possible.
     *
     * @param node Relational expression
     * @return Rewritten query
     */
    public RelNode rewrite( RelNode node ) {
        return RelOptMaterialization.tryUseStar( node, starRelOptTable );
    }


    /**
     * Retrieves a materialized table that will satisfy an aggregate query on the star table.
     *
     * The current implementation creates a materialization and populates it, provided that {@link Lattice#auto} is true.
     *
     * Future implementations might return materializations at a different level of aggregation, from which the desired result can be obtained by rolling up.
     *
     * @param planner Current planner
     * @param groupSet Grouping key
     * @param measureList Calls to aggregate functions
     * @return Materialized table
     */
    public Pair<PolyphenyDbSchema.TableEntry, TileKey> getAggregate( RelOptPlanner planner, ImmutableBitSet groupSet, List<Lattice.Measure> measureList ) {
        final PolyphenyDbConnectionConfig config = planner.getContext().unwrap( PolyphenyDbConnectionConfig.class );
        if ( config == null ) {
            return null;
        }
        final MaterializationService service = MaterializationService.instance();
        boolean create = lattice.auto && config.createMaterializations();
        final PolyphenyDbSchema schema = starRelOptTable.unwrap( PolyphenyDbSchema.class );
        return service.defineTile( lattice, groupSet, measureList, schema, create, false );
    }
}


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
 */

package org.polypheny.db.rel.logical;


import java.util.List;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.ConditionalExecute;


public class LogicalConditionalExecute extends ConditionalExecute {

    public LogicalConditionalExecute( RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, Condition condition ) {
        super( cluster, traitSet, left, right, condition );
    }


    public static LogicalConditionalExecute create( RelNode left, RelNode right, Condition condition ) {
        return new LogicalConditionalExecute( right.getCluster(), right.getTraitSet(), left, right, condition );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new LogicalConditionalExecute( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), inputs.get( 1 ), condition );
    }


}
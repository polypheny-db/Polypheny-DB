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

package org.polypheny.db.adapter.enumerable;


import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.rel.logical.LogicalModifyCollect;
import org.polypheny.db.rel.logical.LogicalUnion;


/**
 * Rule to convert an {@link LogicalUnion} to an {@link EnumerableUnion}.
 */
class EnumerableModifyCollectRule extends ConverterRule {

    EnumerableModifyCollectRule() {
        super( LogicalModifyCollect.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableModifyCollectRule" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        final LogicalModifyCollect union = (LogicalModifyCollect) rel;
        final EnumerableConvention out = EnumerableConvention.INSTANCE;
        final RelTraitSet traitSet = union.getTraitSet().replace( out );
        return new EnumerableModifyCollect( rel.getCluster(), traitSet, convertList( union.getInputs(), out ), true );
    }
}


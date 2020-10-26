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

package org.polypheny.db.adapter.cottontail.rel;


import org.apache.calcite.linq4j.tree.DeclarationStatement;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.cottontail.CottontailConvention;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Data;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


public class CottontailValues extends Values implements org.polypheny.db.adapter.cottontail.rel.CottontailRel {


    public static final Type DATA_MAP_TYPE = TypeUtils.parameterize( HashMap.class, String.class, Data.class );
    public static final Type LIST_DATA_MAPS_TYPE = TypeUtils.parameterize( ArrayList.class, DATA_MAP_TYPE );

//    private static final Method MAP_PUT_METHOD = Types.lookupMethod( Map.class, "put",  )

    public CottontailValues( RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits ) {
        super( cluster, rowType, tuples, traits );
    }

    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( CottontailConvention.COST_MULTIPLIER );
    }


    @Override
    public void implement( CottontailImplementContext context ) {

        BlockBuilder builder = context.blockBuilder;


//        final ParameterExpression valuesMapList_ = Expressions.parameter( Modifier.FINAL, LIST_DATA_MAPS_TYPE, builder.newName( "valuesMapList" ) );
        final ParameterExpression valuesMapList_ = Expressions.parameter( ArrayList.class, builder.newName( "valuesMapList" ) );
        final NewExpression valuesMapListCreator = Expressions.new_( ArrayList.class );
        builder.add( Expressions.declare( Modifier.FINAL, valuesMapList_, valuesMapListCreator ) );


        final List<String> physicalColumnNames = new ArrayList<>();
        for ( RelDataTypeField field : this.rowType.getFieldList() ) {
            physicalColumnNames.add( context.cottontailTable.getPhysicalColumnName( field.getName() ) );
        }

        for ( List<RexLiteral> tuple : tuples ) {
            final ParameterExpression valuesMap_ = Expressions.variable( Map.class, builder.newName( "valuesMap" ) );
            final NewExpression valuesMapCreator = Expressions.new_( HashMap.class );
            builder.add( Expressions.declare( Modifier.FINAL, valuesMap_, valuesMapCreator ) );

            for ( Pair<String, RexLiteral> pair : Pair.zip( physicalColumnNames, tuple ) ) {
                builder.add(
                        Expressions.statement(
                        Expressions.call( valuesMap_,
                                BuiltInMethod.MAP_PUT.method,
                                Expressions.constant( pair.left ),
                                CottontailTypeUtil.rexLiteralToDataExpression( pair.right ) ) )
                );
            }

            builder.add( Expressions.statement ( Expressions.call( valuesMapList_, Types.lookupMethod( List.class, "add", Object.class), valuesMap_ ) ) );
        }

        context.blockBuilder = builder;
        context.valuesHashMapList = valuesMapList_;
    }


    static class Translator {
        private final RelDataType rowType;
        private final List<String> fieldNames;


        public Translator( RelDataType rowType ) {
            this.rowType = rowType;
            List<Pair<String, String>> pairs = Pair.zip( rowType.getFieldList().stream().map( RelDataTypeField::getPhysicalName ).collect( Collectors.toList() ), rowType.getFieldNames() );
            this.fieldNames = pairs.stream().map( it -> it.left != null ? it.left : it.right ).collect( Collectors.toList() );
        }


    }
}

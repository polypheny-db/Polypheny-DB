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

package org.polypheny.db.test.catalog;


import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql2rel.InitializerContext;
import org.polypheny.db.sql2rel.InitializerExpressionFactory;
import org.polypheny.db.sql2rel.NullInitializerExpressionFactory;


/**
 * To check whether {@link InitializerExpressionFactory#newColumnDefaultValue} is called.
 *
 * If a column is in {@code defaultColumns}, returns 1 as the default value.
 */
public class CountingFactory extends NullInitializerExpressionFactory {

    public static final ThreadLocal<AtomicInteger> THREAD_CALL_COUNT = ThreadLocal.withInitial( AtomicInteger::new );

    private final List<String> defaultColumns;


    CountingFactory( List<String> defaultColumns ) {
        this.defaultColumns = ImmutableList.copyOf( defaultColumns );
    }


    @Override
    public ColumnStrategy generationStrategy( RelOptTable table, int iColumn ) {
        final RelDataTypeField field = table.getRowType().getFieldList().get( iColumn );
        if ( defaultColumns.contains( field.getName() ) ) {
            return ColumnStrategy.DEFAULT;
        }
        return super.generationStrategy( table, iColumn );
    }


    @Override
    public RexNode newColumnDefaultValue( RelOptTable table, int iColumn, InitializerContext context ) {
        THREAD_CALL_COUNT.get().incrementAndGet();
        final RelDataTypeField field = table.getRowType().getFieldList().get( iColumn );
        if ( defaultColumns.contains( field.getName() ) ) {
            final RexBuilder rexBuilder = context.getRexBuilder();
            return rexBuilder.makeExactLiteral( BigDecimal.ONE );
        }
        return super.newColumnDefaultValue( table, iColumn, context );
    }


    @Override
    public RexNode newAttributeInitializer( RelDataType type, SqlFunction constructor, int iAttribute, List<RexNode> constructorArgs, InitializerContext context ) {
        THREAD_CALL_COUNT.get().incrementAndGet();
        return super.newAttributeInitializer( type, constructor, iAttribute, constructorArgs, context );
    }
}


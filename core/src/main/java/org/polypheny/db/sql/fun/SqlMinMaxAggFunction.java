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

package org.polypheny.db.sql.fun;


import com.google.common.base.Preconditions;
import org.polypheny.db.sql.SqlAggFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlSplittableAggFunction;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Optionality;


/**
 * Definition of the <code>MIN</code> and <code>MAX</code> aggregate functions, returning the returns the smallest/largest of the values which go into it.
 *
 * There are 3 forms:
 *
 * <dl>
 * <dt>sum(<em>primitive type</em>)</dt>
 * <dd>values are compared using '&lt;'</dd>
 *
 * <dt>sum({@link java.lang.Comparable})</dt>
 * <dd>values are compared using {@link java.lang.Comparable#compareTo}</dd>
 *
 * <dt>sum({@link java.util.Comparator}, {@link java.lang.Object})</dt>
 * <dd>the {@link java.util.Comparator#compare} method of the comparator is used to compare pairs of objects. The comparator is a startup argument, and must therefore be constant for the duration of the aggregation.</dd>
 * </dl>
 */
public class SqlMinMaxAggFunction extends SqlAggFunction {

    /**
     * Creates a SqlMinMaxAggFunction.
     */
    public SqlMinMaxAggFunction( SqlKind kind ) {
        super(
                kind.name(),
                null,
                kind,
                ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
                null,
                OperandTypes.COMPARABLE_ORDERED,
                SqlFunctionCategory.SYSTEM,
                false,
                false,
                Optionality.FORBIDDEN );
        Preconditions.checkArgument( kind == SqlKind.MIN || kind == SqlKind.MAX );
    }


    @Override
    public <T> T unwrap( Class<T> clazz ) {
        if ( clazz == SqlSplittableAggFunction.class ) {
            return clazz.cast( SqlSplittableAggFunction.SelfSplitter.INSTANCE );
        }
        return super.unwrap( clazz );
    }
}


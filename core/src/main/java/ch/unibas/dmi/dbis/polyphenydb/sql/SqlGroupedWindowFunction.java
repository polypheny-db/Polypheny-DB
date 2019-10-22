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

package ch.unibas.dmi.dbis.polyphenydb.sql;


import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlReturnTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlMonotonicity;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * SQL function that computes keys by which rows can be partitioned and aggregated.
 *
 * Grouped window functions always occur in the GROUP BY clause. They often have auxiliary functions that access information about the group. For example, {@code HOP} is a group function, and its auxiliary functions are
 * {@code HOP_START} and {@code HOP_END}. Here they are used in a streaming query:
 *
 * <blockquote><pre>
 * SELECT STREAM HOP_START(rowtime, INTERVAL '1' HOUR),
 *   HOP_END(rowtime, INTERVAL '1' HOUR),
 *   MIN(unitPrice)
 * FROM Orders
 * GROUP BY HOP(rowtime, INTERVAL '1' HOUR), productId
 * </pre></blockquote>
 */
public class SqlGroupedWindowFunction extends SqlFunction {

    /**
     * The grouped function, if this an auxiliary function; null otherwise.
     */
    public final SqlGroupedWindowFunction groupFunction;


    /**
     * Creates a SqlGroupedWindowFunction.
     *
     * @param name Function name
     * @param kind Kind
     * @param groupFunction Group function, if this is an auxiliary; null, if this is a group function
     * @param returnTypeInference Strategy to use for return type inference
     * @param operandTypeInference Strategy to use for parameter type inference
     * @param operandTypeChecker Strategy to use for parameter type checking
     * @param category Categorization for function
     */
    public SqlGroupedWindowFunction(
            String name,
            SqlKind kind,
            SqlGroupedWindowFunction groupFunction,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory category ) {
        super( name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category );
        this.groupFunction = groupFunction;
        Preconditions.checkArgument( groupFunction == null || groupFunction.groupFunction == null );
    }


    /**
     * Creates an auxiliary function from this grouped window function.
     *
     * @param kind Kind; also determines function name
     */
    public SqlGroupedWindowFunction auxiliary( SqlKind kind ) {
        return auxiliary( kind.name(), kind );
    }


    /**
     * Creates an auxiliary function from this grouped window function.
     *
     * @param name Function name
     * @param kind Kind
     */
    public SqlGroupedWindowFunction auxiliary( String name, SqlKind kind ) {
        return new SqlGroupedWindowFunction( name, kind, this, ReturnTypes.ARG0, null, getOperandTypeChecker(), SqlFunctionCategory.SYSTEM );
    }


    /**
     * Returns a list of this grouped window function's auxiliary functions.
     */
    public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
        return ImmutableList.of();
    }


    @Override
    public boolean isGroup() {
        // Auxiliary functions are not group functions
        return groupFunction == null;
    }


    @Override
    public boolean isGroupAuxiliary() {
        return groupFunction != null;
    }


    @Override
    public SqlMonotonicity getMonotonicity( SqlOperatorBinding call ) {
        // Monotonic iff its first argument is, but not strict.
        //
        // Note: This strategy happens to works for all current group functions (HOP, TUMBLE, SESSION). When there are exceptions to this rule, we'll make the method abstract.
        return call.getOperandMonotonicity( 0 ).unstrict();
    }
}

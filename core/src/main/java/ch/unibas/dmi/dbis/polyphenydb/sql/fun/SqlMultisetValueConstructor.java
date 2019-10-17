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

package ch.unibas.dmi.dbis.polyphenydb.sql.fun;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCallBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSpecialOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.InferTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeUtil;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import java.util.List;


/**
 * Definition of the SQL:2003 standard MULTISET constructor, <code>MULTISET [&lt;expr&gt;, ...]</code>.
 *
 * Derived classes construct other kinds of collections.
 *
 * @see SqlMultisetQueryConstructor
 */
public class SqlMultisetValueConstructor extends SqlSpecialOperator {


    public SqlMultisetValueConstructor() {
        this( "MULTISET", SqlKind.MULTISET_VALUE_CONSTRUCTOR );
    }


    protected SqlMultisetValueConstructor( String name, SqlKind kind ) {
        super(
                name,
                kind, MDX_PRECEDENCE,
                false,
                ReturnTypes.ARG0,
                InferTypes.FIRST_KNOWN,
                OperandTypes.VARIADIC );
    }


    @Override
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        RelDataType type =
                getComponentType(
                        opBinding.getTypeFactory(),
                        opBinding.collectOperandTypes() );
        if ( null == type ) {
            return null;
        }
        return SqlTypeUtil.createMultisetType(
                opBinding.getTypeFactory(),
                type,
                false );
    }


    protected RelDataType getComponentType( RelDataTypeFactory typeFactory, List<RelDataType> argTypes ) {
        return typeFactory.leastRestrictive( argTypes );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final List<RelDataType> argTypes =
                SqlTypeUtil.deriveAndCollectTypes(
                        callBinding.getValidator(),
                        callBinding.getScope(),
                        callBinding.operands() );
        if ( argTypes.size() == 0 ) {
            throw callBinding.newValidationError( Static.RESOURCE.requireAtLeastOneArg() );
        }
        final RelDataType componentType = getComponentType( callBinding.getTypeFactory(), argTypes );
        if ( null == componentType ) {
            if ( throwOnFailure ) {
                throw callBinding.newValidationError( Static.RESOURCE.needSameTypeParameter() );
            }
            return false;
        }
        return true;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.keyword( getName() ); // "MULTISET" or "ARRAY"
        final SqlWriter.Frame frame = writer.startList( "[", "]" );
        for ( SqlNode operand : call.getOperandList() ) {
            writer.sep( "," );
            operand.unparse( writer, leftPrec, rightPrec );
        }
        writer.endList( frame );
    }
}


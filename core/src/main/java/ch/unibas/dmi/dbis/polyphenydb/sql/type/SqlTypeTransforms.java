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

package ch.unibas.dmi.dbis.polyphenydb.sql.type;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.util.List;
import java.util.Objects;


/**
 * SqlTypeTransforms defines a number of reusable instances of {@link SqlTypeTransform}.
 *
 * NOTE: avoid anonymous inner classes here except for unique, non-generalizable strategies; anything else belongs in a reusable top-level class. If you find yourself copying and pasting an existing strategy's
 * anonymous inner class, you're making a mistake.
 */
public abstract class SqlTypeTransforms {

    /**
     * Parameter type-inference transform strategy where a derived type is transformed into the same type but nullable if any of a calls operands is nullable
     */
    public static final SqlTypeTransform TO_NULLABLE =
            ( opBinding, typeToTransform ) ->
                    SqlTypeUtil.makeNullableIfOperandsAre(
                            opBinding.getTypeFactory(),
                            opBinding.collectOperandTypes(),
                            Objects.requireNonNull( typeToTransform ) );

    /**
     * Parameter type-inference transform strategy where a derived type is transformed into the same type, but nullable if and only if all of a call's operands are nullable.
     */
    public static final SqlTypeTransform TO_NULLABLE_ALL = ( opBinding, type ) -> {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        return typeFactory.createTypeWithNullability( type, SqlTypeUtil.allNullable( opBinding.collectOperandTypes() ) );
    };

    /**
     * Parameter type-inference transform strategy where a derived type is transformed into the same type but not nullable.
     */
    public static final SqlTypeTransform TO_NOT_NULLABLE =
            ( opBinding, typeToTransform ) ->
                    opBinding.getTypeFactory().createTypeWithNullability(
                            Objects.requireNonNull( typeToTransform ),
                            false );

    /**
     * Parameter type-inference transform strategy where a derived type is transformed into the same type with nulls allowed.
     */
    public static final SqlTypeTransform FORCE_NULLABLE =
            ( opBinding, typeToTransform ) ->
                    opBinding.getTypeFactory().createTypeWithNullability(
                            Objects.requireNonNull( typeToTransform ),
                            true );

    /**
     * Type-inference strategy whereby the result is NOT NULL if any of the arguments is NOT NULL; otherwise the type is unchanged.
     */
    public static final SqlTypeTransform LEAST_NULLABLE =
            ( opBinding, typeToTransform ) -> {
                for ( RelDataType type : opBinding.collectOperandTypes() ) {
                    if ( !type.isNullable() ) {
                        return opBinding.getTypeFactory().createTypeWithNullability( typeToTransform, false );
                    }
                }
                return typeToTransform;
            };

    /**
     * Type-inference strategy whereby the result type of a call is VARYING the type given. The length returned is the same as length of the first argument. Return type will have same nullability as input type
     * nullability. First Arg must be of string type.
     */
    public static final SqlTypeTransform TO_VARYING =
            new SqlTypeTransform() {
                @Override
                public RelDataType transformType( SqlOperatorBinding opBinding, RelDataType typeToTransform ) {
                    switch ( typeToTransform.getSqlTypeName() ) {
                        case VARCHAR:
                        case VARBINARY:
                            return typeToTransform;
                    }

                    SqlTypeName retTypeName = toVar( typeToTransform );

                    RelDataType ret = opBinding.getTypeFactory().createSqlType( retTypeName, typeToTransform.getPrecision() );
                    if ( SqlTypeUtil.inCharFamily( typeToTransform ) ) {
                        ret = opBinding.getTypeFactory()
                                .createTypeWithCharsetAndCollation(
                                        ret,
                                        typeToTransform.getCharset(),
                                        typeToTransform.getCollation() );
                    }
                    return opBinding.getTypeFactory().createTypeWithNullability( ret, typeToTransform.isNullable() );
                }


                private SqlTypeName toVar( RelDataType type ) {
                    final SqlTypeName sqlTypeName = type.getSqlTypeName();
                    switch ( sqlTypeName ) {
                        case CHAR:
                            return SqlTypeName.VARCHAR;
                        case BINARY:
                            return SqlTypeName.VARBINARY;
                        case ANY:
                            return SqlTypeName.ANY;
                        default:
                            throw Util.unexpected( sqlTypeName );
                    }
                }
            };

    /**
     * Parameter type-inference transform strategy where a derived type must be a multiset type and the returned type is the multiset's element type.
     *
     * @see MultisetSqlType#getComponentType
     */
    public static final SqlTypeTransform TO_MULTISET_ELEMENT_TYPE = ( opBinding, typeToTransform ) -> typeToTransform.getComponentType();

    /**
     * Parameter type-inference transform strategy that wraps a given type in a multiset.
     *
     * @see RelDataTypeFactory#createMultisetType(RelDataType, long)
     */
    public static final SqlTypeTransform TO_MULTISET = ( opBinding, typeToTransform ) -> opBinding.getTypeFactory().createMultisetType( typeToTransform, -1 );

    /**
     * Parameter type-inference transform strategy where a derived type must be a struct type with precisely one field and the returned type is the type of that field.
     */
    public static final SqlTypeTransform ONLY_COLUMN =
            ( opBinding, typeToTransform ) -> {
                final List<RelDataTypeField> fields = typeToTransform.getFieldList();
                assert fields.size() == 1;
                return fields.get( 0 ).getType();
            };
}


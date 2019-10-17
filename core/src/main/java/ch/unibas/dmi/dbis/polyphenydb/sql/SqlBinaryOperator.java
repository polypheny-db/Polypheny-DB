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


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlReturnTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlMonotonicity;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.math.BigDecimal;
import java.nio.charset.Charset;


/**
 * <code>SqlBinaryOperator</code> is a binary operator.
 */
public class SqlBinaryOperator extends SqlOperator {


    /**
     * Creates a SqlBinaryOperator.
     *
     * @param name Name of operator
     * @param kind Kind
     * @param prec Precedence
     * @param leftAssoc Left-associativity
     * @param returnTypeInference Strategy to infer return type
     * @param operandTypeInference Strategy to infer operand types
     * @param operandTypeChecker Validator for operand types
     */
    public SqlBinaryOperator(
            String name,
            SqlKind kind,
            int prec,
            boolean leftAssoc,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker ) {
        super(
                name,
                kind,
                leftPrec( prec, leftAssoc ),
                rightPrec( prec, leftAssoc ),
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker );
    }


    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.BINARY;
    }


    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        Util.discard( operandsCount );

        // op0 opname op1
        return "{1} {0} {2}";
    }


    /**
     * {@inheritDoc}
     *
     * Returns true for most operators but false for the '.' operator; consider
     *
     * <blockquote>
     * <pre>x.y + 5 * 6</pre>
     * </blockquote>
     */
    @Override
    boolean needsSpace() {
        return !getName().equals( "." );
    }


    @Override
    protected RelDataType adjustType( SqlValidator validator, final SqlCall call, RelDataType type ) {
        RelDataType operandType1 = validator.getValidatedNodeType( call.operand( 0 ) );
        RelDataType operandType2 = validator.getValidatedNodeType( call.operand( 1 ) );
        if ( SqlTypeUtil.inCharFamily( operandType1 ) && SqlTypeUtil.inCharFamily( operandType2 ) ) {
            Charset cs1 = operandType1.getCharset();
            Charset cs2 = operandType2.getCharset();
            assert (null != cs1) && (null != cs2) : "An implicit or explicit charset should have been set";
            if ( !cs1.equals( cs2 ) ) {
                throw validator.newValidationError( call, Static.RESOURCE.incompatibleCharset( getName(), cs1.name(), cs2.name() ) );
            }

            SqlCollation col1 = operandType1.getCollation();
            SqlCollation col2 = operandType2.getCollation();
            assert (null != col1) && (null != col2) : "An implicit or explicit collation should have been set";

            // validation will occur inside getCoercibilityDyadicOperator...
            SqlCollation resultCol = SqlCollation.getCoercibilityDyadicOperator( col1, col2 );

            if ( SqlTypeUtil.inCharFamily( type ) ) {
                type = validator.getTypeFactory().createTypeWithCharsetAndCollation( type, type.getCharset(), resultCol );
            }
        }
        return type;
    }


    @Override
    public RelDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        RelDataType type = super.deriveType( validator, scope, call );

        RelDataType operandType1 = validator.getValidatedNodeType( call.operand( 0 ) );
        RelDataType operandType2 = validator.getValidatedNodeType( call.operand( 1 ) );
        if ( SqlTypeUtil.inCharFamily( operandType1 ) && SqlTypeUtil.inCharFamily( operandType2 ) ) {
            Charset cs1 = operandType1.getCharset();
            Charset cs2 = operandType2.getCharset();
            assert (null != cs1) && (null != cs2) : "An implicit or explicit charset should have been set";
            if ( !cs1.equals( cs2 ) ) {
                throw validator.newValidationError( call, Static.RESOURCE.incompatibleCharset( getName(), cs1.name(), cs2.name() ) );
            }

            SqlCollation col1 = operandType1.getCollation();
            SqlCollation col2 = operandType2.getCollation();
            assert (null != col1) && (null != col2) : "An implicit or explicit collation should have been set";

            // validation will occur inside getCoercibilityDyadicOperator...
            SqlCollation resultCol = SqlCollation.getCoercibilityDyadicOperator( col1, col2 );

            if ( SqlTypeUtil.inCharFamily( type ) ) {
                type = validator.getTypeFactory()
                        .createTypeWithCharsetAndCollation( type, type.getCharset(), resultCol );
            }
        }
        return type;
    }


    @Override
    public SqlMonotonicity getMonotonicity( SqlOperatorBinding call ) {
        if ( getName().equals( "/" ) ) {
            final SqlMonotonicity mono0 = call.getOperandMonotonicity( 0 );
            final SqlMonotonicity mono1 = call.getOperandMonotonicity( 1 );
            if ( mono1 == SqlMonotonicity.CONSTANT ) {
                if ( call.isOperandLiteral( 1, false ) ) {
                    switch ( call.getOperandLiteralValue( 1, BigDecimal.class ).signum() ) {
                        case -1:
                            // mono / -ve constant --> reverse mono, unstrict
                            return mono0.reverse().unstrict();

                        case 0:
                            // mono / zero --> constant (infinity!)
                            return SqlMonotonicity.CONSTANT;

                        default:
                            // mono / +ve constant * mono1 --> mono, unstrict
                            return mono0.unstrict();
                    }
                }
            }
        }

        return super.getMonotonicity( call );
    }


    @Override
    public boolean validRexOperands( int count, Litmus litmus ) {
        if ( count != 2 ) {
            // Special exception for AND and OR.
            if ( (this == SqlStdOperatorTable.AND || this == SqlStdOperatorTable.OR) && count > 2 ) {
                return true;
            }
            return litmus.fail( "wrong operand count {} for {}", count, this );
        }
        return litmus.succeed();
    }
}


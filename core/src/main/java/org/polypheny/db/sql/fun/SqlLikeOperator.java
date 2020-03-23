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


import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperandCountRange;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.sql.parser.SqlParserUtil;
import org.polypheny.db.type.InferTypes;
import org.polypheny.db.type.OperandTypes;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.ReturnTypes;


/**
 * An operator describing the <code>LIKE</code> and <code>SIMILAR</code> operators.
 *
 * Syntax of the two operators:
 *
 * <ul>
 * <li><code>src-value [NOT] LIKE pattern-value [ESCAPE escape-value]</code></li>
 * <li><code>src-value [NOT] SIMILAR pattern-value [ESCAPE escape-value]</code></li>
 * </ul>
 *
 * <b>NOTE</b> If the <code>NOT</code> clause is present the {@link org.polypheny.db.sql.parser.SqlParser parser} will generate a equivalent to <code>NOT (src LIKE pattern ...)</code>
 */
public class SqlLikeOperator extends SqlSpecialOperator {

    private final boolean negated;


    /**
     * Creates a SqlLikeOperator.
     *
     * @param name Operator name
     * @param kind Kind
     * @param negated Whether this is 'NOT LIKE'
     */
    SqlLikeOperator( String name, SqlKind kind, boolean negated ) {
        // LIKE is right-associative, because that makes it easier to capture dangling ESCAPE clauses: "a like b like c escape d" becomes "a like (b like c escape d)".
        super(
                name,
                kind,
                32,
                false,
                ReturnTypes.BOOLEAN_NULLABLE,
                InferTypes.FIRST_KNOWN,
                OperandTypes.STRING_SAME_SAME_SAME );
        this.negated = negated;
    }


    /**
     * Returns whether this is the 'NOT LIKE' operator.
     *
     * @return whether this is 'NOT LIKE'
     */
    public boolean isNegated() {
        return negated;
    }


    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.between( 2, 3 );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        switch ( callBinding.getOperandCount() ) {
            case 2:
                if ( !OperandTypes.STRING_SAME_SAME.checkOperandTypes( callBinding, throwOnFailure ) ) {
                    return false;
                }
                break;
            case 3:
                if ( !OperandTypes.STRING_SAME_SAME_SAME.checkOperandTypes( callBinding, throwOnFailure ) ) {
                    return false;
                }

                // calc implementation should enforce the escape character length to be 1
                break;
            default:
                throw new AssertionError( "unexpected number of args to " + callBinding.getCall() + ": " + callBinding.getOperandCount() );
        }

        return PolyTypeUtil.isCharTypeComparable( callBinding, callBinding.operands(), throwOnFailure );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startList( "", "" );
        call.operand( 0 ).unparse( writer, getLeftPrec(), getRightPrec() );
        writer.sep( getName() );

        call.operand( 1 ).unparse( writer, getLeftPrec(), getRightPrec() );
        if ( call.operandCount() == 3 ) {
            writer.sep( "ESCAPE" );
            call.operand( 2 ).unparse( writer, getLeftPrec(), getRightPrec() );
        }
        writer.endList( frame );
    }


    @Override
    public ReduceResult reduceExpr( final int opOrdinal, TokenSequence list ) {
        // Example:
        //   a LIKE b || c ESCAPE d || e AND f
        // |  |    |      |      |      |
        //  exp0    exp1          exp2
        SqlNode exp0 = list.node( opOrdinal - 1 );
        SqlOperator op = list.op( opOrdinal );
        assert op instanceof SqlLikeOperator;
        SqlNode exp1 = SqlParserUtil.toTreeEx(
                list,
                opOrdinal + 1,
                getRightPrec(),
                SqlKind.ESCAPE );
        SqlNode exp2 = null;
        if ( (opOrdinal + 2) < list.size() ) {
            if ( list.isOp( opOrdinal + 2 ) ) {
                final SqlOperator op2 = list.op( opOrdinal + 2 );
                if ( op2.getKind() == SqlKind.ESCAPE ) {
                    exp2 = SqlParserUtil.toTreeEx(
                            list,
                            opOrdinal + 3,
                            getRightPrec(),
                            SqlKind.ESCAPE );
                }
            }
        }
        final SqlNode[] operands;
        int end;
        if ( exp2 != null ) {
            operands = new SqlNode[]{ exp0, exp1, exp2 };
            end = opOrdinal + 4;
        } else {
            operands = new SqlNode[]{ exp0, exp1 };
            end = opOrdinal + 2;
        }
        SqlCall call = createCall( SqlParserPos.ZERO, operands );
        return new ReduceResult( opOrdinal - 1, end, call );
    }
}

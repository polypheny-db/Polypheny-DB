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


import com.google.common.collect.ImmutableList;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlBinaryOperator;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperandCountRange;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.type.InferTypes;
import org.polypheny.db.type.OperandTypes;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolySingleOperandTypeChecker;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.ReturnTypes;


/**
 * SqlOverlapsOperator represents the SQL:1999 standard {@code OVERLAPS} function. Determines whether two anchored time intervals overlap.
 */
public class SqlOverlapsOperator extends SqlBinaryOperator {


    SqlOverlapsOperator( SqlKind kind ) {
        super(
                kind.sql,
                kind,
                30,
                true,
                ReturnTypes.BOOLEAN_NULLABLE,
                InferTypes.FIRST_KNOWN,
                OperandTypes.sequence( "'<PERIOD> " + kind.sql + " <PERIOD>'", OperandTypes.PERIOD, OperandTypes.PERIOD ) );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE );
        arg( writer, call, leftPrec, rightPrec, 0 );
        writer.sep( getName() );
        arg( writer, call, leftPrec, rightPrec, 1 );
        writer.endList( frame );
    }


    void arg( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, int i ) {
        if ( SqlUtil.isCallTo( call.operand( i ), SqlStdOperatorTable.ROW ) ) {
            SqlCall row = call.operand( i );
            writer.keyword( "PERIOD" );
            writer.sep( "(", true );
            row.operand( 0 ).unparse( writer, leftPrec, rightPrec );
            writer.sep( ",", true );
            row.operand( 1 ).unparse( writer, leftPrec, rightPrec );
            writer.sep( ")", true );
        } else {
            call.operand( i ).unparse( writer, leftPrec, rightPrec );
        }
    }


    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.of( 2 );
    }


    @Override
    public String getAllowedSignatures( String opName ) {
        final String d = "DATETIME";
        final String i = "INTERVAL";
        String[] typeNames = {
                d, d,
                d, i,
                i, d,
                i, i
        };

        StringBuilder ret = new StringBuilder();
        for ( int y = 0; y < typeNames.length; y += 2 ) {
            if ( y > 0 ) {
                ret.append( NL );
            }
            ret.append( SqlUtil.getAliasedSignature( this, opName, ImmutableList.of( d, typeNames[y], d, typeNames[y + 1] ) ) );
        }
        return ret.toString();
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        if ( !OperandTypes.PERIOD.checkSingleOperandType( callBinding, callBinding.operand( 0 ), 0, throwOnFailure ) ) {
            return false;
        }
        final PolySingleOperandTypeChecker rightChecker;
        switch ( kind ) {
            case CONTAINS:
                rightChecker = OperandTypes.PERIOD_OR_DATETIME;
                break;
            default:
                rightChecker = OperandTypes.PERIOD;
                break;
        }
        if ( !rightChecker.checkSingleOperandType( callBinding, callBinding.operand( 1 ), 0, throwOnFailure ) ) {
            return false;
        }
        final RelDataType t0 = callBinding.getOperandType( 0 );
        final RelDataType t1 = callBinding.getOperandType( 1 );
        if ( !PolyTypeUtil.isDatetime( t1 ) ) {
            final RelDataType t00 = t0.getFieldList().get( 0 ).getType();
            final RelDataType t10 = t1.getFieldList().get( 0 ).getType();
            if ( !PolyTypeUtil.sameNamedType( t00, t10 ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }
        }
        return true;
    }
}


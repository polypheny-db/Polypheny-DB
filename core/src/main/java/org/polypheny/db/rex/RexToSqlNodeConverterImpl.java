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

package org.polypheny.db.rex;


import org.polypheny.db.sql.SqlLiteral;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;


/**
 * Standard implementation of {@link RexToSqlNodeConverter}.
 */
public class RexToSqlNodeConverterImpl implements RexToSqlNodeConverter {

    private final RexSqlConvertletTable convertletTable;


    public RexToSqlNodeConverterImpl( RexSqlConvertletTable convertletTable ) {
        this.convertletTable = convertletTable;
    }


    // implement RexToSqlNodeConverter
    @Override
    public SqlNode convertNode( RexNode node ) {
        if ( node instanceof RexLiteral ) {
            return convertLiteral( (RexLiteral) node );
        } else if ( node instanceof RexInputRef ) {
            return convertInputRef( (RexInputRef) node );
        } else if ( node instanceof RexCall ) {
            return convertCall( (RexCall) node );
        }
        return null;
    }


    // implement RexToSqlNodeConverter
    @Override
    public SqlNode convertCall( RexCall call ) {
        final RexSqlConvertlet convertlet = convertletTable.get( call );
        if ( convertlet != null ) {
            return convertlet.convertCall( this, call );
        }

        return null;
    }


    // implement RexToSqlNodeConverter
    @Override
    public SqlNode convertLiteral( RexLiteral literal ) {
        // Numeric
        if ( PolyTypeFamily.EXACT_NUMERIC.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createExactNumeric(
                    literal.getValue().toString(),
                    SqlParserPos.ZERO );
        }

        if ( PolyTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createApproxNumeric(
                    literal.getValue().toString(),
                    SqlParserPos.ZERO );
        }

        // Timestamp
        if ( PolyTypeFamily.TIMESTAMP.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createTimestamp(
                    literal.getValueAs( TimestampString.class ),
                    0,
                    SqlParserPos.ZERO );
        }

        // Date
        if ( PolyTypeFamily.DATE.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createDate(
                    literal.getValueAs( DateString.class ),
                    SqlParserPos.ZERO );
        }

        // Time
        if ( PolyTypeFamily.TIME.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createTime(
                    literal.getValueAs( TimeString.class ),
                    0,
                    SqlParserPos.ZERO );
        }

        // String
        if ( PolyTypeFamily.CHARACTER.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createCharString(
                    ((NlsString) (literal.getValue())).getValue(),
                    SqlParserPos.ZERO );
        }

        // Boolean
        if ( PolyTypeFamily.BOOLEAN.getTypeNames().contains( literal.getTypeName() ) ) {
            return SqlLiteral.createBoolean(
                    (Boolean) literal.getValue(),
                    SqlParserPos.ZERO );
        }

        // Null
        if ( PolyTypeFamily.NULL == literal.getTypeName().getFamily() ) {
            return SqlLiteral.createNull( SqlParserPos.ZERO );
        }

        return null;
    }


    // implement RexToSqlNodeConverter
    @Override
    public SqlNode convertInputRef( RexInputRef ref ) {
        return null;
    }
}


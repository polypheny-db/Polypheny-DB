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

package org.polypheny.db.sql;


import com.google.common.base.Preconditions;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.sql.type.SqlTypeName;
import org.polypheny.db.util.TimeString;


/**
 * A SQL literal representing a TIME value, for example <code>TIME '14:33:44.567'</code>.
 *
 * Create values using {@link SqlLiteral#createTime}.
 */
public class SqlTimeLiteral extends SqlAbstractDateTimeLiteral {


    SqlTimeLiteral( TimeString t, int precision, boolean hasTimeZone, SqlParserPos pos ) {
        super( t, hasTimeZone, SqlTypeName.TIME, precision, pos );
        Preconditions.checkArgument( this.precision >= 0 );
    }


    /**
     * Converts this literal to a {@link TimeString}.
     */
    protected TimeString getTime() {
        return (TimeString) value;
    }


    @Override
    public SqlTimeLiteral clone( SqlParserPos pos ) {
        return new SqlTimeLiteral( (TimeString) value, precision, hasTimeZone, pos );
    }


    public String toString() {
        return "TIME '" + toFormattedString() + "'";
    }


    /**
     * Returns e.g. '03:05:67.456'.
     */
    @Override
    public String toFormattedString() {
        return getTime().toString( precision );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.getDialect().unparseDateTimeLiteral( writer, this, leftPrec, rightPrec );
    }
}


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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory.Builder;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWithItem;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;


/**
 * Very similar to {@link AliasNamespace}.
 */
class WithItemNamespace extends AbstractNamespace {

    private final SqlWithItem withItem;


    WithItemNamespace( SqlValidatorImpl validator, SqlWithItem withItem, SqlNode enclosingNode ) {
        super( validator, enclosingNode );
        this.withItem = withItem;
    }


    @Override
    protected RelDataType validateImpl( RelDataType targetRowType ) {
        final SqlValidatorNamespace childNs = validator.getNamespace( withItem.query );
        final RelDataType rowType = childNs.getRowTypeSansSystemColumns();
        if ( withItem.columnList == null ) {
            return rowType;
        }
        final Builder builder = validator.getTypeFactory().builder();
        for ( Pair<SqlNode, RelDataTypeField> pair : Pair.zip( withItem.columnList, rowType.getFieldList() ) ) {
            builder.add( ((SqlIdentifier) pair.left).getSimple(), null, pair.right.getType() );
        }
        return builder.build();
    }


    @Override
    public SqlNode getNode() {
        return withItem;
    }


    @Override
    public String translate( String name ) {
        if ( withItem.columnList == null ) {
            return name;
        }
        final RelDataType underlyingRowType = validator.getValidatedNodeType( withItem.query );
        int i = 0;
        for ( RelDataTypeField field : rowType.getFieldList() ) {
            if ( field.getName().equals( name ) ) {
                return underlyingRowType.getFieldList().get( i ).getName();
            }
            ++i;
        }
        throw new AssertionError( "unknown field '" + name + "' in rowtype " + underlyingRowType );
    }
}


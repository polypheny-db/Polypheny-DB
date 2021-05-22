/*
 * Copyright 2019-2021 The Polypheny Project
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
 */

package org.polypheny.db.adapter.mongodb.bson;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.gridfs.GridFSBucket;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.adapter.mongodb.MongoRowType;
import org.polypheny.db.adapter.mongodb.util.MongoTypeUtil;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlKind;

public class BsonFunctionHelper extends BsonDocument {

    static String l2Function = "function(arr1, arr2){return Math.pow((arr1[0]-arr2[0]),2)+Math.pow((arr1[1]-arr2[1]),2)}";
    static String l2squaredFunction = "function(arr1, arr2){return Math.sqrt(Math.pow((arr1[0]-arr2[0]),2)+Math.pow((arr1[1]-arr2[1]),2))}";
    static String l1Function = "function(arr1, arr2){return Math.abs((arr1[0]-arr2[0]),2)+Math.abs((arr1[1]-arr2[1]),2)}";


    public static BsonDocument getFunction( RexCall call, MongoRowType rowType, GridFSBucket bucket ) {
        String function;
        if ( call.operands.size() == 3 && call.operands.get( 2 ) instanceof RexLiteral ) {
            Object funcName = ((RexLiteral) call.operands.get( 2 )).getValue3();
            if ( funcName.equals( "L2SQUARED" ) ) {
                function = l2Function;
            } else if ( funcName.equals( "L2" ) ) {
                function = l2squaredFunction;
            } else if ( funcName.equals( "L1" ) ) {
                function = l1Function;
            } else {
                throw new IllegalArgumentException( "Unsupported function for MongoDB" );
            }

            BsonArray args = getArgsArray( call.operands, rowType, bucket );

            return new BsonDocument().append( "$function",
                    new BsonDocument()
                            .append( "body", new BsonString( function ) )
                            .append( "args", args )
                            .append( "lang", new BsonString( "js" ) ) );

        }
        throw new IllegalArgumentException( "Unsupported function for MongoDB" );

    }


    private static BsonArray getArgsArray( ImmutableList<RexNode> operands, MongoRowType rowType, GridFSBucket bucket ) {
        BsonArray array = new BsonArray();
        if ( operands.size() == 3 && operands.get( 2 ) instanceof RexLiteral ) {
            array.add( getVal( operands.get( 0 ), rowType, bucket ) );
            array.add( getVal( operands.get( 1 ), rowType, bucket ) );

            return array;
        }
        throw new IllegalArgumentException( "This function is not supported yet" );

    }


    private static BsonValue getVal( RexNode rexNode, MongoRowType rowType, GridFSBucket bucket ) {
        if ( rexNode.isA( SqlKind.INPUT_REF ) ) {
            RexInputRef rex = (RexInputRef) rexNode;
            return new BsonString( "$" + rowType.getPhysicalName( rowType.getFieldNames().get( rex.getIndex() ) ) );

        } else if ( rexNode.isA( SqlKind.ARRAY_VALUE_CONSTRUCTOR ) ) {
            RexCall rex = (RexCall) rexNode;
            return MongoTypeUtil.getBsonArray( rex, bucket );

        } else if ( rexNode.isA( SqlKind.DYNAMIC_PARAM ) ) {
            RexDynamicParam rex = (RexDynamicParam) rexNode;
            return new BsonDynamic( rex );
        }
        throw new IllegalArgumentException( "This function argument is not supported yet" );
    }

}

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

package org.polypheny.db.adapter.mongodb;

import com.google.common.collect.ImmutableList;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonValue;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;

public class BsonFunction extends BsonDocument {

    private final BsonJavaScriptWithScope function;
    private final MongoRowType rowType;


    public BsonFunction( RexCall call, MongoRowType mongoRowType ) {
        this.rowType = mongoRowType;
        switch ( call.op.kind ) {
            case DISTANCE:
                BsonDocument scope = new BsonDocument();
                scope.append( "args", getArgsArray( call.operands ) );

                this.function = new BsonJavaScriptWithScope( "function(arr1, arr2){return Math.pow((arr1[0]-arr2[0]),2)+Math.pow((arr1[1]-arr2[1]),2)", scope );
                break;
            default:
                throw new IllegalStateException( "Unexpected value: " + call.op );
        }
    }


    private BsonArray getArgsArray( ImmutableList<RexNode> operands ) {
        BsonArray array = new BsonArray();
        if ( operands.size() == 3 && operands.get( 2 ) instanceof RexLiteral && ((RexLiteral) operands.get( 2 )).getValue3().equals( "L2SQUARED" ) ) {
            array.add( getVal( operands.get( 0 ) ) );
            array.add( getVal( operands.get( 1 ) ) );
        }
        throw new IllegalArgumentException( "This function is not supported yet" );

    }


    private BsonValue getVal( RexNode rexNode ) {
        /*if( rexNode.isA( RexInputRef)  ) {

        }else if (odb){

        }*/
        throw new IllegalArgumentException( "This function argument is not supported yet" );
    }

}

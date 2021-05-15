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

package org.polypheny.db.adapter.mongodb.util;

import com.mongodb.client.gridfs.GridFSBucket;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.adapter.mongodb.MongoTypeUtil;
import org.polypheny.db.type.PolyType;

public class MongoDynamicUtil {


    public static BsonValue replaceDynamic( BsonValue preDocument, Map<Long, Object> parameterValues, GridFSBucket bucket ) {
        if ( preDocument instanceof BsonDocument ) {
            if ( ((BsonDocument) preDocument).containsKey( "_dyn" ) ) {
                // prepared
                BsonValue bsonIndex = ((BsonDocument) preDocument).get( "_dyn" );
                long index;
                if ( bsonIndex.isInt64() ) {
                    index = bsonIndex.asInt64().getValue();
                } else {
                    index = bsonIndex.asInt32().getValue();
                }
                String polyTypeName = ((BsonDocument) preDocument).get( "_type" ).asString().getValue();

                return MongoTypeUtil.getAsBson( parameterValues.get( index ), PolyType.valueOf( polyTypeName ), bucket );

            } else {
                // normal
                BsonDocument doc = new BsonDocument();
                ((BsonDocument) preDocument).forEach( ( s, bsonValue ) -> doc.append( s, replaceDynamic( bsonValue, parameterValues, bucket ) ) );
                return doc;
            }
        } else if ( preDocument instanceof BsonArray ) {
            BsonArray array = new BsonArray();
            ((BsonArray) preDocument).forEach( bsonValue -> array.add( replaceDynamic( bsonValue, parameterValues, bucket ) ) );
            return array;
        }

        return preDocument;
    }


    public static BsonDocument initReplace( BsonDocument preDocument, Map<Long, Object> parameterValues, GridFSBucket bucket ) {
        if ( parameterValues.size() == 0 ) {
            return preDocument;
        }
        BsonDocument doc = new BsonDocument();
        preDocument.forEach( (( s, bsonValue ) -> doc.put( s, replaceDynamic( bsonValue, parameterValues, bucket ) )) );
        return doc;

    }


}

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

import com.mongodb.client.gridfs.GridFSBucket;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Objects;
import lombok.SneakyThrows;
import org.apache.calcite.avatica.util.ByteString;
import org.bson.BsonBoolean;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;

public class MongoTypeUtil {

    @SneakyThrows
    public static BsonValue getAsBson( Object obj, BasicPolyType type, GridFSBucket bucket ) {
        BsonValue value;
        if ( type.getPolyType() == PolyType.NULL ) {
            value = new BsonNull();
        } else if ( type.getFamily() == PolyTypeFamily.CHARACTER ) {
            value = new BsonString( Objects.requireNonNull( obj.toString() ) );
        } else if ( PolyType.INT_TYPES.contains( type.getPolyType() ) ) {
            value = new BsonInt32( (Integer) obj );
        } else if ( PolyType.FRACTIONAL_TYPES.contains( type.getPolyType() ) ) {
            value = new BsonDouble( (Double) obj );
        } else if ( type.getFamily() == PolyTypeFamily.DATE || type.getFamily() == PolyTypeFamily.TIME ) {
            value = new BsonInt32( (Integer) obj );
        } else if ( type.getFamily() == PolyTypeFamily.TIMESTAMP ) {
            value = new BsonInt64( (Long) obj );
        } else if ( type.getFamily() == PolyTypeFamily.BOOLEAN ) {
            value = new BsonBoolean( (Boolean) obj );
        } else if ( type.getFamily() == PolyTypeFamily.BINARY ) {
            value = new BsonString( ((ByteString) obj).toBase64String() );
        } else if ( type.getFamily() == PolyTypeFamily.ARRAY ) {
            if ( obj == null ) {
                value = new BsonNull();
            } else {
                throw new RuntimeException( "Arrays are not yet fully supported for the MongoDB Adapter" ); // todo dl: add array with content
            }
        } else if ( PolyTypeFamily.MULTIMEDIA == type.getPolyType().getFamily() ) {
            int length = ((InputStream) obj).available();
            ObjectId id = bucket.uploadFromStream( "test", (InputStream) obj );
            value = new BsonDocument().append( "_id", new BsonString( id.toString() ) );

        } else {
            value = new BsonString( obj.toString() );
        }
        return value;
    }


    public static BsonValue getAsBson( Object obj, GridFSBucket bucket ) {
        if ( obj == null ) {
            return new BsonNull();
        } else if ( obj instanceof String ) {
            return new BsonString( (String) obj );
        } else if ( obj instanceof Integer ) {
            return new BsonInt32( (Integer) obj );
        } else if ( obj instanceof Long ) {
            return new BsonInt64( (Long) obj );
        } else if ( obj instanceof BigDecimal ) {
            return new BsonDecimal128( new Decimal128( (BigDecimal) obj ) );
        } else if ( obj instanceof Double ) {
            return new BsonDouble( (Double) obj );
        } else if ( obj instanceof Time || obj instanceof Date ) {
            return new BsonInt32( (Integer) obj );
        } else if ( obj instanceof Timestamp ) {
            return new BsonInt64( (Long) obj );
        } else if ( obj instanceof Boolean ) {
            return new BsonBoolean( (Boolean) obj );
        } else if ( obj instanceof ByteString ) {
            return new BsonString( ((ByteString) obj).toBase64String() );
            // add array
        } else if ( obj instanceof InputStream ) {
            // the object is a file which need to be handle specially
            ObjectId id = bucket.uploadFromStream( "test", (PushbackInputStream) obj );
            return new BsonDocument().append( "_id", new BsonString( id.toString() ) );
        } else {
            return new BsonString( obj.toString() );
        }
    }

}

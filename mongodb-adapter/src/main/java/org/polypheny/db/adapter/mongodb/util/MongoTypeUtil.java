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
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.avatica.util.ByteString;
import org.bson.BsonArray;
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
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;

public class MongoTypeUtil {

    public static Function<Object, BsonValue> getBsonTransformer( PolyType type, GridFSBucket bucket ) {
        Function<Object, BsonValue> function = getBsonTransformerSpecial( type, bucket );
        return ( o ) -> {
            if ( o == null ) {
                return new BsonNull();
            } else if ( o instanceof List ) {
                BsonArray array = new BsonArray();
                ((List<?>) o).forEach( el -> array.add( getAsBson( el, type, bucket ) ) );
                return array;
            } else {
                return function.apply( o );
            }
        };
    }


    private static Function<Object, BsonValue> getBsonTransformerSpecial( PolyType type, GridFSBucket bucket ) {
        Function<Object, BsonValue> function;
        if ( type.getFamily() == PolyTypeFamily.CHARACTER ) {
            function = ( o ) -> new BsonString( o.toString() );
        } else if ( type == PolyType.BIGINT ) {
            function = ( o ) -> new BsonInt64( (Long) o );
        } else if ( type == PolyType.DECIMAL ) {
            function = ( o ) -> {
                if ( o instanceof String ) {
                    return new BsonDecimal128( new Decimal128( new BigDecimal( (String) o ) ) );
                } else {
                    return new BsonDecimal128( new Decimal128( new BigDecimal( String.valueOf( o ) ) ) );
                }
            };

        } else if ( type == PolyType.TINYINT ) {
            function = ( o ) -> {
                if ( o instanceof Long ) {
                    return new BsonInt32( Math.toIntExact( (Long) o ) );
                } else {
                    return new BsonInt32( (Byte) o );
                }
            };
        } else if ( type == PolyType.SMALLINT ) {
            function = ( o ) -> {
                if ( o instanceof Long ) {
                    return new BsonInt32( Math.toIntExact( (Long) o ) );
                } else if ( o instanceof Integer ) {
                    return new BsonInt32( (Integer) o );
                } else {
                    return new BsonInt32( (Short) o );
                }
            };

        } else if ( PolyType.INT_TYPES.contains( type ) ) {
            function = ( o ) -> new BsonInt32( (Integer) o );
        } else if ( type == PolyType.FLOAT || type == PolyType.REAL ) {
            function = ( o ) -> new BsonDouble( Double.parseDouble( o.toString() ) );
        } else if ( PolyType.FRACTIONAL_TYPES.contains( type ) ) {
            function = ( o ) -> new BsonDouble( (Double) o );
        } else if ( type.getFamily() == PolyTypeFamily.DATE ) {
            function = ( o ) -> {
                if ( o instanceof Integer ) {
                    return new BsonInt64( (Integer) o );
                } else if ( o instanceof Date ) {
                    return new BsonInt64( ((Date) o).toLocalDate().toEpochDay() );
                } else {
                    return new BsonInt64( new Date( ((Time) o).getTime() ).toLocalDate().toEpochDay() );
                }
            };
        } else if ( type.getFamily() == PolyTypeFamily.TIME ) {
            function = ( o ) -> {
                if ( o instanceof Integer ) {
                    return new BsonInt64( ((Integer) o) );
                } else {
                    return new BsonInt64( ((Time) o).toLocalTime().toNanoOfDay() / 1000000 );
                }
            };
        } else if ( type.getFamily() == PolyTypeFamily.TIMESTAMP ) {
            function = ( o ) -> {
                if ( o instanceof Timestamp ) {
                    return new BsonInt64( ((Timestamp) o).toInstant().toEpochMilli() + 3600000 );
                } else {
                    return new BsonInt64( (Long) o );
                }
            };
        } else if ( type.getFamily() == PolyTypeFamily.BOOLEAN ) {
            function = ( o ) -> new BsonBoolean( (Boolean) o );
        } else if ( type.getFamily() == PolyTypeFamily.BINARY ) {
            function = ( o ) -> new BsonString( ((ByteString) o).toBase64String() );
        } else if ( PolyTypeFamily.MULTIMEDIA == type.getFamily() ) {
            function = ( o ) -> {
                ObjectId id = bucket.uploadFromStream( "_", (InputStream) o );
                return new BsonDocument()
                        .append( "_type", new BsonString( "s" ) )
                        .append( "_id", new BsonString( id.toString() ) );
            };
        } else {
            function = ( o ) -> new BsonString( o.toString() );
        }
        return function;
    }


    public static BsonValue getAsBson( Object obj, PolyType type, GridFSBucket bucket ) {
        BsonValue value;
        if ( obj instanceof List ) { // TODO DL: reevaluate
            BsonArray array = new BsonArray();
            ((List<?>) obj).forEach( el -> array.add( getAsBson( el, type, bucket ) ) );
            value = array;
        } else if ( type == PolyType.NULL || obj == null ) {
            value = new BsonNull();
        } else if ( type.getFamily() == PolyTypeFamily.CHARACTER ) {
            value = new BsonString( obj.toString() );
        } else if ( type == PolyType.BIGINT ) {
            value = new BsonInt64( (Long) obj );
        } else if ( type == PolyType.DECIMAL ) {
            if ( obj instanceof String ) {
                value = new BsonDecimal128( new Decimal128( new BigDecimal( (String) obj ) ) );
            } else {
                value = new BsonDecimal128( new Decimal128( new BigDecimal( String.valueOf( obj ) ) ) );
            }
        } else if ( type == PolyType.TINYINT ) {
            if ( obj instanceof Long ) {
                value = new BsonInt32( Math.toIntExact( (Long) obj ) );
            } else {
                value = new BsonInt32( (Byte) obj );
            }
        } else if ( type == PolyType.SMALLINT ) {
            if ( obj instanceof Long ) {
                value = new BsonInt32( Math.toIntExact( (Long) obj ) );
            } else if ( obj instanceof Integer ) {
                value = new BsonInt32( (Integer) obj );
            } else {
                value = new BsonInt32( (Short) obj );
            }

        } else if ( PolyType.INT_TYPES.contains( type ) ) {
            value = new BsonInt32( (Integer) obj );
        } else if ( type == PolyType.FLOAT || type == PolyType.REAL ) {
            value = new BsonDouble( Double.parseDouble( obj.toString() ) );
        } else if ( PolyType.FRACTIONAL_TYPES.contains( type ) ) {
            value = new BsonDouble( (Double) obj );
        } else if ( type.getFamily() == PolyTypeFamily.DATE ) {
            if ( obj instanceof Integer ) {
                value = new BsonInt64( (Integer) obj );
            } else if ( obj instanceof Date ) {
                value = new BsonInt64( ((Date) obj).toLocalDate().toEpochDay() );
            } else {
                value = new BsonInt64( new Date( ((Time) obj).getTime() ).toLocalDate().toEpochDay() );
            }
        } else if ( type.getFamily() == PolyTypeFamily.TIME ) {
            //value = new BsonTimestamp( ((Time) obj).getTime() );
            if ( obj instanceof Integer ) {
                value = new BsonInt64( ((Integer) obj) );
            } else {
                value = new BsonInt64( ((Time) obj).toLocalTime().toNanoOfDay() / 1000000 ); // TODO DL: why not getEpoch?
            }
        } else if ( type.getFamily() == PolyTypeFamily.TIMESTAMP ) {
            if ( obj instanceof Timestamp ) {
                //value = new BsonTimestamp( ((Timestamp) obj).getTime() );
                value = new BsonInt64( ((Timestamp) obj).toInstant().toEpochMilli() + 3600000 ); // todo dl fix
            } else {
                value = new BsonInt64( (Long) obj );
            }
        } else if ( type.getFamily() == PolyTypeFamily.BOOLEAN ) {
            value = new BsonBoolean( (Boolean) obj );
        } else if ( type.getFamily() == PolyTypeFamily.BINARY ) {
            value = new BsonString( ((ByteString) obj).toBase64String() );
        } else if ( PolyTypeFamily.MULTIMEDIA == type.getFamily() ) {
            ObjectId id = bucket.uploadFromStream( "_", (InputStream) obj );
            value = new BsonDocument()
                    .append( "_type", new BsonString( "s" ) )
                    .append( "_id", new BsonString( id.toString() ) );
        } else {
            value = new BsonString( obj.toString() );
        }
        return value;
    }


    public static BsonValue getAsBson( RexLiteral literal, GridFSBucket bucket ) {
        return getAsBson( getMongoComparable( literal.getType().getPolyType(), literal ), literal.getType().getPolyType(), bucket );
    }


    public static Comparable<?> getMongoComparable( PolyType finalType, RexLiteral el ) {
        if ( el.getValue() == null ) {
            return null;
        }

        switch ( finalType ) {

            case BOOLEAN:
                return el.getValueAs( Boolean.class );
            case TINYINT:
                return el.getValueAs( Byte.class );
            case SMALLINT:
                return el.getValueAs( Short.class );
            case INTEGER:
                return el.getValueAs( Integer.class );
            case BIGINT:
                return el.getValueAs( Long.class );
            case DECIMAL:
                return el.getValueAs( BigDecimal.class ).toString();
            case FLOAT:
            case REAL:
                return el.getValueAs( Float.class );
            case DOUBLE:
                return el.getValueAs( Double.class );
            case DATE:
            case TIME:
                return el.getValueAs( Integer.class );
            case TIMESTAMP:
                return el.getValueAs( Long.class );
            case CHAR:
            case VARCHAR:
                return el.getValueAs( String.class );
            case GEOMETRY:
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                return el.getValueAs( ByteString.class ).toBase64String();
            default:
                return el.getValue();
        }
    }


    public static BsonArray getBsonArray( RexCall call, GridFSBucket bucket ) {
        BsonArray array = new BsonArray();
        for ( RexNode op : call.operands ) {
            if ( op instanceof RexCall ) {
                array.add( getBsonArray( (RexCall) op, bucket ) );
            } else {
                array.add( getAsBson( ((RexLiteral) op).getValue3(), ((RexLiteral) op).getTypeName(), bucket ) );
            }
        }
        return array;
    }


    public static Class<?> getClassFromType( PolyType type ) {
        switch ( type ) {

            case BOOLEAN:
                return Boolean.class;
            case TINYINT:
                return Short.class;
            case SMALLINT:
            case INTEGER:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case DECIMAL:
                return BigDecimal.class;
            case FLOAT:
            case REAL:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case DATE:
                return Date.class;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return Time.class;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.class;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                throw new RuntimeException( "Interval is not supported yet" );
            case CHAR:
            case VARCHAR:
                return String.class;
            case BINARY:
            case VARBINARY:
                return String.class;
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                return PushbackInputStream.class;
            default:
                throw new IllegalStateException( "Unexpected value: " + type );
        }
    }


    public static BsonValue replaceRegex( String input ) {

        if ( !input.startsWith( "%" ) ) {
            input = "^" + input;
        }

        if ( !input.endsWith( "%" ) ) {
            input = input + "$";
        }

        input = input
                .replace( "_", "." )
                .replace( "%", ".*" );

        return new BsonDocument()
                .append( "$regex", new BsonString( input ) )
                // Polypheny is case insensitive and therefore we have to set the "i" option
                .append( "$options", new BsonString( "i" ) );
    }

}

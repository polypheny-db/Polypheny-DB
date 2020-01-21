/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
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
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra;


import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaRecordType;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Values;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactoryImpl.JavaType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelRecordType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.runtime.GeoFunctions;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.BasicSqlType;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.IntervalSqlType;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Expression;


@Slf4j
public class CassandraValues extends Values implements CassandraRel {

    protected CassandraValues( RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits ) {
        super( cluster, rowType, tuples, traits );
    }


    /**
     * Convert the value of a literal to a string.
     *
     * @param literal Literal to translate
     * @return String representation of the literal
     */
    public static Object literalValue( RexLiteral literal ) {
//        Object value = literal.getValue2();
//        return String.valueOf( value );
        Object valueType = getJavaClass( literal);
        return valueType;
//        return value;
    }


    public static Literal wrappedLiteral( RexLiteral literal ) {
        RelDataType type = literal.getType();
        if ( type instanceof BasicSqlType || type instanceof IntervalSqlType ) {
            switch ( type.getSqlTypeName() ) {
                case VARCHAR:
                case CHAR:
                    return QueryBuilder.literal((String) literal.getValue2());
                case DATE:
                    try {
                        log.info( "Attempting to convert date." );
                        Calendar daysSinceEpoch = (Calendar) literal.getValue();
                        return QueryBuilder.literal( LocalDate.ofInstant( daysSinceEpoch.toInstant(), daysSinceEpoch.getTimeZone().toZoneId() ) );
                    } catch ( Exception e ) {
                        log.error( "Unable to cast date. ", e );
                        throw new RuntimeException( e );
                    }
//                    return QueryBuilder.literal(LocalDate.ofEpochDay(daysSinceEpoch));
                case TIME:
                case TIME_WITH_LOCAL_TIME_ZONE:
                case INTEGER:
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                    return QueryBuilder.literal((Integer) literal.getValue2());
//                    return type.isNullable() ? Integer.class : int.class;
                case TIMESTAMP:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case BIGINT:
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
                    return QueryBuilder.literal((Long) literal.getValue2());
//                    return type.isNullable() ? Long.class : long.class;
                case SMALLINT:
                    return QueryBuilder.literal((Short) literal.getValue2());
//                    return type.isNullable() ? Short.class : short.class;
                case TINYINT:
                    return QueryBuilder.literal((Byte) literal.getValue2());
//                    return type.isNullable() ? Byte.class : byte.class;
                case DECIMAL:
                    return QueryBuilder.literal((BigDecimal) literal.getValue2());
//                    return BigDecimal.class;
                case BOOLEAN:
                    return QueryBuilder.literal((Boolean) literal.getValue2());
//                    return type.isNullable() ? Boolean.class : boolean.class;
                case DOUBLE:
                case FLOAT: // sic
                    return QueryBuilder.literal((Double) literal.getValue2());
//                    return type.isNullable() ? Double.class : double.class;
                case REAL:
                    return QueryBuilder.literal((Float) literal.getValue2());
//                    return type.isNullable() ? Float.class : float.class;
                case BINARY:
                case VARBINARY:
                    return QueryBuilder.literal((ByteString) literal.getValue2());
//                    return ByteString.class;
                case GEOMETRY:
//                    return GeoFunctions.Geom.class;
                case SYMBOL:
//                    return Enum.class;
                case ANY:
                    return QueryBuilder.literal(literal.getValue2());
            }
        }
        return null;
    }


    public static Object getJavaClass( RexLiteral literal ) {
        RelDataType type = literal.getType();
        if ( type instanceof BasicSqlType || type instanceof IntervalSqlType ) {
            switch ( type.getSqlTypeName() ) {
                case VARCHAR:
                case CHAR:
                    return literal.getValue2();
                case DATE:
                case TIME:
                case TIME_WITH_LOCAL_TIME_ZONE:
                    try {
                        log.info( "Attempting to convert date." );
                        Calendar daysSinceEpoch = (Calendar) literal.getValue();
                        return LocalDate.ofInstant( daysSinceEpoch.toInstant(), daysSinceEpoch.getTimeZone().toZoneId() );
                    } catch ( Exception e ) {
                        log.error( "Unable to cast date. ", e );
                        throw new RuntimeException( e );
                    }
                case INTEGER:
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                    return literal.getValue2();
//                    return type.isNullable() ? Integer.class : int.class;
                case TIMESTAMP:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    try {
                        log.info( "Attempting to convert date." );
                        Calendar daysSinceEpoch = (Calendar) literal.getValue();
                        return LocalDate.ofInstant( daysSinceEpoch.toInstant(), daysSinceEpoch.getTimeZone().toZoneId() );
                    } catch ( Exception e ) {
                        log.error( "Unable to cast date. ", e );
                        throw new RuntimeException( e );
                    }
                case BIGINT:
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
                    return (Long) literal.getValue2();
//                    return type.isNullable() ? Long.class : long.class;
                case SMALLINT:
                    return (Short) literal.getValue2();
//                    return type.isNullable() ? Short.class : short.class;
                case TINYINT:
                    return (Byte) literal.getValue2();
//                    return type.isNullable() ? Byte.class : byte.class;
                case DECIMAL:
                    return (BigDecimal) literal.getValue();
//                    return BigDecimal.class;
                case BOOLEAN:
                    return (Boolean) literal.getValue2();
//                    return type.isNullable() ? Boolean.class : boolean.class;
                case DOUBLE:
                case FLOAT: // sic
                    return (Double) literal.getValue2();
//                    return type.isNullable() ? Double.class : double.class;
                case REAL:
                    return (Float) literal.getValue2();
//                    return type.isNullable() ? Float.class : float.class;
                case BINARY:
                case VARBINARY:
                    return (ByteString) literal.getValue2();
//                    return ByteString.class;
                case GEOMETRY:
//                    return GeoFunctions.Geom.class;
                case SYMBOL:
//                    return Enum.class;
                case ANY:
                    return Object.class;
            }
        }
        return null;
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( CassandraConvention.COST_MULTIPLIER );
    }


    @Override
    public void implement( Implementor implementor ) {

        List<Map<String, Term>> items = new LinkedList<>();
        final List<RelDataTypeField> fields = rowType.getFieldList();
        for ( List<RexLiteral> tuple : tuples ) {
            final List<Expression> literals = new ArrayList<>();
            Map<String, Term> oneInsert = new LinkedHashMap<>();
            for ( Pair<RelDataTypeField, RexLiteral> pair : Pair.zip( fields, tuple ) ) {
                try {
                    oneInsert.put( pair.left.getName(), QueryBuilder.literal( literalValue( pair.right ) ) );
                } catch ( Exception e ) {
                    log.error( "Something broke while parsing cql values.", e );
                    throw new RuntimeException( e );
                }
//                oneInsert.put( pair.left.getName(), wrappedLiteral( pair.right ) );
            }

            items.add( oneInsert );
        }

        implementor.addInsertValues( items );
    }
}

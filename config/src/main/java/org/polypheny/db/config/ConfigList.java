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

package org.polypheny.db.config;

import com.google.gson.annotations.SerializedName;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.polypheny.db.config.exception.ConfigRuntimeException;

/**
 * In contrast to Arrays, List of different Objects have the same erasure to Java
 * to counteract that, we have to use a different pattern as in the other ConfigTypes
 */
public class ConfigList extends Config {

    @SerializedName("values")
    List<ConfigScalar> list;

    ConfigScalar template;

    /**
     * listener which propagates the changes to underlying configs to
     * listeners of this config
     */
    ConfigListener listener = new ConfigListener() {
        @Override
        public void onConfigChange( Config c ) {
            notifyConfigListeners();
        }


        @Override
        public void restart( Config c ) {
            notifyConfigListeners();
        }
    };


    // while java does not known if our cast are correct
    // we know, so we can suppress here
    @SuppressWarnings("unchecked")
    public ConfigList( String key, final List<?> list, Class<?> clazz ) {
        super( key );
        this.webUiFormType = WebUiFormType.LIST;
        if ( clazz.equals( String.class ) ) {
            stringList( key, (List<String>) list );
        } else if ( clazz.equals( Integer.class ) ) {
            integerList( key, (List<Integer>) list );
        } else if ( clazz.equals( Double.class ) ) {
            doubleList( key, (List<Double>) list );
        } else if ( clazz.equals( Long.class ) ) {
            longList( key, (List<Long>) list );
        } else if ( clazz.equals( BigDecimal.class ) ) {
            decimalList( key, (List<BigDecimal>) list );
        } else if ( clazz.equals( Boolean.class ) ) {
            booleanList( key, (List<Boolean>) list );
        } else if ( clazz.equals( ConfigDocker.class ) ) {
            dockerList( key, (List<ConfigDocker>) list );
        } else {
            throw new UnsupportedOperationException( "A ConfigList, which uses that that type is not supported." );
        }
    }


    public void integerList( String key, final List<Integer> list ) {
        List<ConfigScalar> fill = new ArrayList<>();
        for ( int i = 0; i < list.size(); i++ ) {
            fill.add( (ConfigScalar) new ConfigInteger( key + "." + i, list.get( i ) ).isObservable( false ) );
        }

        this.template = (ConfigScalar) new ConfigInteger( key + ".", 0 ).isObservable( false );
        this.list = fill;
    }


    public void doubleList( String key, final List<Double> list ) {
        List<ConfigScalar> fill = new ArrayList<>();
        for ( int i = 0; i < list.size(); i++ ) {
            fill.add( (ConfigScalar) new ConfigDouble( key + "." + i, list.get( i ) ).isObservable( false ) );
        }

        this.template = (ConfigScalar) new ConfigDouble( key + ".", 0 ).isObservable( false );
        this.list = fill;
    }


    public void longList( String key, final List<Long> list ) {
        List<ConfigScalar> fill = new ArrayList<>();
        for ( int i = 0; i < list.size(); i++ ) {
            fill.add( (ConfigScalar) new ConfigLong( key + "." + i, list.get( i ) ).isObservable( false ) );
        }

        this.template = (ConfigScalar) new ConfigLong( key + ".", 0 ).isObservable( false );
        this.list = fill;
    }


    public void decimalList( String key, final List<BigDecimal> list ) {
        List<ConfigScalar> fill = new ArrayList<>();
        for ( int i = 0; i < list.size(); i++ ) {
            fill.add( (ConfigScalar) new ConfigDecimal( key + "." + i, list.get( i ) ).isObservable( false ) );
        }

        this.template = (ConfigScalar) new ConfigDecimal( key + ".", BigDecimal.ONE ).isObservable( false );
        this.list = fill;
    }


    public void stringList( String key, final List<String> list ) {
        List<ConfigScalar> fill = new ArrayList<>();
        for ( int i = 0; i < list.size(); i++ ) {
            fill.add( (ConfigScalar) new ConfigString( key + "." + i, list.get( i ) ).isObservable( false ) );
        }

        this.template = (ConfigScalar) new ConfigString( key + ".", "" ).isObservable( false );
        this.list = fill;
    }


    public void booleanList( String key, final List<Boolean> list ) {
        List<ConfigScalar> fill = new ArrayList<>();
        for ( int i = 0; i < list.size(); i++ ) {
            fill.add( (ConfigScalar) new ConfigBoolean( key + "." + i, list.get( i ) ).isObservable( false ) );
        }

        this.template = (ConfigScalar) new ConfigBoolean( key + ".", false ).isObservable( false );
        this.list = fill;
    }


    public void dockerList( String key, final List<ConfigDocker> list ) {
        this.template = new ConfigDocker( "localhost", null, null );
        this.list = list.stream().map( el -> (ConfigScalar) el ).collect( Collectors.toList() );
    }


    @Override
    public <T> List<T> getList( Class<T> type ) {
        return list.stream().map( type::cast ).collect( Collectors.toList() );
    }


    @Override
    public List<String> getStringList() {
        return list.stream().map( Config::getString ).collect( Collectors.toList() );
    }


    @Override
    public List<Integer> getIntegerList() {
        return list.stream().map( Config::getInt ).collect( Collectors.toList() );
    }


    @Override
    public List<Double> getDoubleList() {
        return list.stream().map( Config::getDouble ).collect( Collectors.toList() );
    }


    @Override
    public List<Long> getStringLong() {
        return list.stream().map( Config::getLong ).collect( Collectors.toList() );
    }


    @Override
    public List<BigDecimal> getDecimalList() {
        return list.stream().map( Config::getDecimal ).collect( Collectors.toList() );
    }


    @Override
    public List<Boolean> getBooleanList() {
        return list.stream().map( Config::getBoolean ).collect( Collectors.toList() );
    }


    @Override
    public Class<? extends ConfigScalar> getTemplateClass() {
        return template.getClass();
    }


    @Override
    public boolean setConfigObjectList( List<Object> values, Class<? extends ConfigScalar> clazz ) {
        BiFunction<String, Object, ? extends ConfigScalar> setter;
        if ( clazz.equals( ConfigString.class ) ) {
            setter = ( key, value ) -> new ConfigString( key, (String) value );
        } else if ( clazz.equals( ConfigInteger.class ) ) {
            setter = ( key, value ) -> new ConfigInteger( key, (Integer) value );
        } else if ( clazz.equals( ConfigDouble.class ) ) {
            setter = ( key, value ) -> new ConfigDouble( key, (Double) value );
        } else if ( clazz.equals( ConfigLong.class ) ) {
            setter = ( key, value ) -> new ConfigLong( key, (Long) value );
        } else if ( clazz.equals( ConfigDecimal.class ) ) {
            setter = ( key, value ) -> new ConfigDecimal( key, (BigDecimal) value );
        } else if ( clazz.equals( ConfigBoolean.class ) ) {
            setter = ( key, value ) -> new ConfigBoolean( key, (Boolean) value );
        } else if ( clazz.equals( ConfigDocker.class ) ) {
            setter = ( key, value ) -> ConfigDocker.fromMap( (Map<String, Object>) value );
        } else {
            return false;
        }
        return setConfigObjectList( values, setter );
    }


    @Override
    public void setList( List<ConfigScalar> values ) {
        this.list = values;
        values.forEach( val -> val.addObserver( listener ) );
        notifyConfigListeners();
    }


    private boolean setConfigObjectList( List<Object> values, BiFunction<String, Object, ? extends ConfigScalar> scalarGetter ) {
        List<ConfigScalar> temp = new ArrayList<>();
        for ( int i = 0; i < values.size(); i++ ) {
            if ( validate( values.get( i ) ) ) {
                Map<String, Object> value = (Map<String, Object>) values.get( i );
                temp.add( i, scalarGetter.apply( (String) value.get( "key" ), value.getOrDefault( "value", value ) ) );

            } else {
                return false;
            }
        }
        this.list.forEach( val -> val.removeObserver( listener ) );

        this.list = temp;

        this.list.forEach( val -> val.addObserver( listener ) );
        notifyConfigListeners();
        return true;
    }


    @Override
    void setValueFromFile( com.typesafe.config.Config conf ) {
        throw new ConfigRuntimeException( "Reading list of values from config files is not supported yet." );
    }


    @Override
    public boolean parseStringAndSetValue( String value ) {
        throw new ConfigRuntimeException( "Parse and set is not implemented for this type." );
    }

}

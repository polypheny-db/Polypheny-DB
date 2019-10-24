/*
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
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.config;


import ch.unibas.dmi.dbis.polyphenydb.config.exception.ConfigRuntimeException;
import com.google.common.collect.ImmutableSet;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.typesafe.config.ConfigException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.reflections.Reflections;


public class ConfigClazz extends Config {

    @JsonAdapter(ClassesAdapter.class)
    private final Set<Class> classes;
    @JsonAdapter(ValueAdapter.class)
    private Class value;


    public ConfigClazz( final String key, final Class superClass, final Class defaultValue ) {
        super( key );
        Reflections reflections = new Reflections( "ch.unibas.dmi.dbis.polyphenydb" );
        //noinspection unchecked
        classes = ImmutableSet.copyOf( reflections.getSubTypesOf( superClass ) );
        setClazz( defaultValue );
    }


    @Override
    public Set<Class> getClazzes() {
        return classes;
    }


    @Override
    public Class getClazz() {
        return value;
    }


    @Override
    public boolean setClazz( final Class value ) {
        if ( classes.contains( value ) ) {
            if ( validate( value ) ) {
                this.value = value;
                notifyConfigListeners();
                return true;
            } else {
                return false;
            }
        } else {
            throw new ConfigRuntimeException( "This class does not implement the specified super class" );
        }
    }


    @Override
    void setValueFromFile( final com.typesafe.config.Config conf ) {
        try {
            setClazz( getByString( conf.getString( this.getKey() ) ) );
        } catch ( ConfigException.Missing e ) {
            // This should have been checked before!
            throw new ConfigRuntimeException( "No config with this key found in the configuration file." );
        } catch ( ConfigException.WrongType e ) {
            throw new ConfigRuntimeException( "The value in the config file has a type which is incompatible with this config element." );
        }

    }


    private Class getByString( String str ) throws ConfigRuntimeException {
        for ( Class c : classes ) {
            if ( str.equalsIgnoreCase( c.getName() ) ) {
                return c;
            }
        }
        throw new ConfigRuntimeException( "No class with name " + str + " found in the list of valid classes." );
    }


    class ClassesAdapter extends TypeAdapter<Set<Class>> {

        @Override
        public void write( final JsonWriter out, final Set<Class> classes ) throws IOException {
            if ( classes == null ) {
                out.nullValue();
                return;
            }
            out.beginArray();
            for ( Class c : classes ) {
                out.value( c.getName() );
            }
            out.endArray();
        }

        @Override
        public Set<Class> read( final JsonReader in ) throws IOException {
            Set<Class> set = new HashSet<>();
            in.beginArray();
            while ( in.hasNext() ) {
                try {
                    Class c = Class.forName( in.nextString() );
                    set.add( c );
                } catch ( ClassNotFoundException e ) {
                    e.printStackTrace();
                    set.add( null );
                }
            }
            in.endArray();
            return ImmutableSet.copyOf( set );
        }
    }


    class ValueAdapter extends TypeAdapter<Class> {

        @Override
        public void write( final JsonWriter out, final Class value ) throws IOException {
            if ( value == null ) {
                out.nullValue();
                return;
            }
            out.value( value.getName() );
        }

        @Override
        public Class read( final JsonReader in ) throws IOException {
            try {
                return Class.forName( in.nextString() );
            } catch ( ClassNotFoundException e ) {
                e.printStackTrace();
                return null;
            }
        }
    }

}

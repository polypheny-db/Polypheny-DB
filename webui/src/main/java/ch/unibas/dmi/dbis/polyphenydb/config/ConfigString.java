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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;


public class ConfigString extends ConfigScalar {

    private String value;


    public ConfigString( final String key, final String value ) {
        super( key );
        this.webUiFormType = WebUiFormType.TEXT;
        this.value = value;
    }


    public ConfigString( final String key, final String description, final String value ) {
        super( key, description );
        this.webUiFormType = WebUiFormType.TEXT;
        this.value = value;
    }


    @Override
    public String getString() {
        return this.value;
    }


    @Override
    public boolean setString( final String s ) {
        if ( validate( s ) ) {
            this.value = s;
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


    @Override
    void setValueFromFile( final Config conf ) {
        final String value;
        try {
            value = conf.getString( this.getKey() ); // read value from config file
        } catch ( ConfigException.Missing e ) {
            // This should have been checked before!
            throw new ConfigRuntimeException( "No config with this key found in the configuration file." );
        } catch ( ConfigException.WrongType e ) {
            throw new ConfigRuntimeException( "The value in the config file has a type which is incompatible with this config element." );
        }
        setString( value );
    }

}

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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;

public class ConfigDocker extends ConfigObject {

    public static final String DEFAULT_PROTOCOL = "tcp";
    public static final int DEFAULT_PORT = 2376;

    // ssh is only usable with keys and not with username/passwort in docker for now,
    // so this is disabled
    public final List<String> protocols = Collections.singletonList( "tcp" );

    @Getter
    @Setter
    private String alias;
    @Getter
    private String url;
    @Getter
    @Setter
    private String protocol = DEFAULT_PROTOCOL;
    @Getter
    @Setter
    private int port = DEFAULT_PORT;
    @Getter
    private String username;
    @Getter
    private String password;
    @Getter
    @Setter
    private boolean dockerRunning = false;


    public ConfigDocker( String url, String username, String password, String alias ) {
        this( idBuilder.getAndIncrement(), url, username, password, alias );
    }


    public ConfigDocker( String url, String username, String password ) {
        this( idBuilder.getAndIncrement(), url, username, password, url );
    }


    public ConfigDocker( int id, String url, String username, String password, String alias ) {
        super( "dockerConfig" + id );
        this.id = id;
        if ( idBuilder.get() <= id ) {
            idBuilder.set( id + 1 );
        }
        this.url = url;
        this.alias = alias;
        this.username = username;
        this.password = password;

        this.webUiFormType = WebUiFormType.DOCKER_INSTANCE;
    }


    public static ConfigDocker fromMap( Map<String, Object> value ) {
        ConfigDocker config = new ConfigDocker(
                ((Double) value.get( "id" )).intValue(),
                (String) value.get( "url" ),
                (String) value.getOrDefault( "username", "" ),
                (String) value.getOrDefault( "password", null ),
                (String) value.get( "alias" ) );
        config.setDockerRunning( (Boolean) value.get( "dockerRunning" ) );
        config.setPort( ((Double) value.getOrDefault( "port", DEFAULT_PORT )).intValue() );
        config.setProtocol( (String) value.getOrDefault( "protocol", DEFAULT_PROTOCOL ) );

        return config;
    }


    @Override
    void setValueFromFile( com.typesafe.config.Config conf ) {
        throw new UnsupportedOperationException( "" );
    }


    @Override
    public boolean parseStringAndSetValue( String value ) {
        return false;
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        ConfigDocker that = (ConfigDocker) o;
        return port == that.port &&
                dockerRunning == that.dockerRunning &&
                url.equals( that.url ) &&
                alias.equals( that.alias ) &&
                protocol.equals( that.protocol ) &&
                Objects.equals( username, that.username ) &&
                Objects.equals( password, that.password );
    }


    @Override
    public int hashCode() {
        return 0;
    }

}

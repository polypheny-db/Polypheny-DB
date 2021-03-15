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

package org.polypheny.db.docker;

import com.github.dockerjava.api.model.ExposedPort;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.docker.Exceptions.NameExistsException;

public abstract class DockerManager {

    public static DockerManagerImpl INSTANCE = null;


    public static DockerManagerImpl getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new DockerManagerImpl();
        }
        return INSTANCE;
    }


    abstract Container createContainer( String uniqueName, int adapterId, Image image, int port, boolean isRestored );

    abstract Container createIfAbsent( String uniqueName, int adapterId, Image image, List<Integer> ports );

    abstract void download( Image image );

    abstract void shutdownAll( int adapterId );

    abstract java.util.HashMap<String, Container> getAvailableContainers();

    abstract List<Image> getAvailableImages();

    abstract HashMap<Integer, ImmutableList<String>> getContainersOnAdapter();

    abstract List<Integer> getUsedPorts();

    abstract List<String> getUsedNames();


    /**
     * This enum unifies the name building and provides additional information of an image
     * If one wants to add a new image it has to be added here for now
     */
    public enum Image {
        MONGODB( "mongo", 27017 );

        @Getter
        private final String name;
        @Getter
        private String version;
        @Getter
        final int internalPort;


        public String getFullName() {
            return this.name + ":" + this.version;
        }


        public Image setVersion( String version ) {
            this.version = version;
            return this;
        }


        Image( String name, String version, int internalPort ) {
            this.name = name;
            this.version = version;
            this.internalPort = internalPort;
        }


        Image( String name, int internalPort ) {
            this.name = name;
            this.version = "latest";
            this.internalPort = internalPort;
        }

    }


    public enum ContainerStatus {
        INIT,
        STOPPED,
        RUNNING,
        ERROR;
    }


    /**
     * The container is the main interaction instance for calling classes when interacting with Docker.
     * It holds all information for a specific Container
     */
    public static class Container {

        public final Image type;
        public final String uniqueName;
        public final Map<Integer, Integer> internalExternalPortMapping;
        public final int adapterId;
        public ContainerStatus status;


        Container(
                int adapterId,
                String uniqueName,
                Image image,
                Map<Integer, Integer> internalExternalPortMapping,
                boolean checkUnique
        ) {
            // check for uniqueness only fires if checkUnique is false here
            if ( checkUnique && !DockerManager.getInstance().checkIfUnique( uniqueName ) ) {
                throw new NameExistsException();
            }
            this.adapterId = adapterId;
            this.type = image;
            this.uniqueName = uniqueName;
            this.internalExternalPortMapping = internalExternalPortMapping;
            this.status = ContainerStatus.INIT;
        }


        public Container start( boolean persistent ) {
            if ( persistent ) {
                DockerManager.getInstance().restart( this );
            } else {
                DockerManager.getInstance().start( this );
            }

            return this;
        }


        public void shutdown() {
            DockerManager.getInstance().shutdown( this );
        }


        public void stop() {
            DockerManager.getInstance().stop( this );
        }


        public List<ExposedPort> getExposedPorts() {
            return internalExternalPortMapping.values().stream().map( ExposedPort::tcp ).collect( Collectors.toList() );
        }


        void setStatus( ContainerStatus status ) {
            this.status = status;
        }

    }

}

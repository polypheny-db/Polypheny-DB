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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.docker.Exceptions.NameExistsException;

/**
 * This class servers as a organization unit which controls all Docker containers in Polypheny.
 * While the callers can and should mostly interact with the underlying containers directly,
 * this instance is used to have a control layer, which allows to restore, start or shutdown multiple of
 * these instances at the same time.
 *
 * For now, we have no way to determent if a previously created/running container with the same name
 * was created by Polypheny, so we try to reuse it
 */
public abstract class DockerManager {

    public static DockerManagerImpl INSTANCE = null;


    public static DockerManagerImpl getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new DockerManagerImpl();
        }
        return INSTANCE;
    }


    /**
     * Tries to download the provided image through Docker,
     * this is necessary to have it accessible when later generating a
     * container from it
     *
     * If the image is already downloaded nothing happens
     *
     * @param image the image which is downloaded
     */
    public abstract void download( Image image );

    /**
     * This method generates a new Polypheny specific Container it additionally initializes said container in Docker itself
     *
     * @param uniqueName the name of the container; has to be unique
     * @param adapterId the adapter to which the container belongs; can be null
     * @param image the image, which is used to generate the container
     * @param externalPort the external port, which uses the predefined port from the image
     * to external (how those can be accessed from outside the container)
     * @return the Container instance
     */
    public abstract Container initialize( String uniqueName, int adapterId, Image image, int externalPort );

    /**
     * This method generates a new Polypheny specific Container it additionally initializes said container in Docker itself
     *
     * @param uniqueName the name of the container; has to be unique
     * @param adapterId the adapter to which the container belongs; can be null
     * @param image the image, which is used to generate the container
     * @param internalExternalPortMapping the mapping of internal ports ( ports which the image uses internally)
     * to external (how those can be accessed from outside the container)
     * @return the Container instance
     */
    public abstract Container initialize( String uniqueName, int adapterId, Image image, Map<Integer, Integer> internalExternalPortMapping );

    /**
     * Starts the provided container,
     * if the container already runs it does nothing,
     * if the container was destroyed it recreates first
     *
     * @param container the container which is started
     */
    public abstract void start( Container container );

    /**
     * All containers, which belong to the provided adapter, are stopped
     *
     * @param adapterId the id of the adapter
     */
    public abstract void stopAll( int adapterId );


    /**
     * Getter for all available containers; except those with ContainerStatus.DESTROYED
     *
     * @return collection with the names and the corresponding container
     */
    public abstract Map<String, Container> getAvailableContainers();

    /**
     * Getter function, which retrieves all downloaded images
     *
     * @return the collection of the images
     */
    public abstract List<Image> getAvailableImages();


    /**
     * Getter, which retrieves a collection, which matches all available containers to their associated adapter
     *
     * @return the collection which maps the ids of the adapters to the containers
     */
    public abstract Map<Integer, ImmutableList<String>> getContainersOnAdapter();

    /**
     * Getter, which returns all already used ports from containers
     *
     * @return the used ports
     */
    public abstract List<Integer> getUsedPorts();

    /**
     * Getter, which returns all already used names of containers
     *
     * @return the used names
     */
    public abstract List<String> getUsedNames();

    /**
     * Destroys all containers and removes them from the system, which belong to the provided adapter
     *
     * @param adapterId the id of the adapter
     */
    public abstract void destroyAll( int adapterId );


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
        ERROR,
        DESTROYED;
    }


    /**
     * The container is the main interaction instance for calling classes when interacting with Docker.
     * It holds all information for a specific Container
     */
    public static class Container {

        public final Image type;
        public final String uniqueName;
        public final Map<Integer, Integer> internalExternalPortMapping;
        public final Integer adapterId;
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


        /**
         * Starts the container
         *
         * @return the started container
         */
        public Container start() {
            DockerManager.getInstance().start( this );

            return this;
        }


        /**
         * Stops the container
         */
        public void stop() {
            DockerManager.getInstance().stop( this );
        }


        /**
         * Destroys the container, which stops and removes it from the system
         */
        public void destroy() {
            DockerManager.getInstance().destroy( this );
        }


        public List<ExposedPort> getExposedPorts() {
            return internalExternalPortMapping.values().stream().map( ExposedPort::tcp ).collect( Collectors.toList() );
        }


        void setStatus( ContainerStatus status ) {
            this.status = status;
        }

    }

}

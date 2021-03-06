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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;

// TODO DL: check if already existing containers in system etc.
// and move to core


/**
 * The class servers as a organization unit which controls all Docker Containers in Polypheny.
 * While the interacting classes mostly interact with the underlying Containers,
 * this instance is used to have a control layer, which allows to restore, start or shutdown multiple of
 * these instances at the same time.
 */
public class DockerManager {

    private final DockerClient client;
    private static DockerManager INSTANCE;
    private final HashMap<String, Container> availableContainers = new HashMap<>();
    private final List<Image> availableImages = new ArrayList<>();
    private final HashMap<Integer, List<String>> containersOnAdapter = new HashMap<>();


    public static DockerManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new DockerManager();
        }
        return INSTANCE;
    }


    private DockerManager() {
        client = this.generateClient();
        client.listImagesCmd().exec().forEach( image -> {
            for ( String tag : image.getRepoTags() ) {
                String[] splits = tag.split( ":" );

                if ( splits[0].equals( Image.MONGODB.getName() ) ) {
                    availableImages.add( Image.MONGODB.setVersion( splits[1] ) );
                }
            }
        } );

        // TODO DL: add existing ports
    }


    private DockerClient generateClient() {
        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost( "tcp://localhost:2375" )
                //.withDockerTlsVerify( true ) //TODO DL: use certificates
                //.withDockerCertPath(certPath)
                .build();

        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost( config.getDockerHost() )
                .sslConfig( config.getSSLConfig() )
                .build();

        DockerClient client = DockerClientImpl.getInstance( config, httpClient );
        client.pingCmd().exec();
        return client;
    }


    protected boolean checkIfUnique( String uniqueName ) {
        return !availableContainers.containsKey( uniqueName );
    }


    private void registerIfAbsent( Container container ) {
        if ( !availableContainers.containsKey( container.uniqueName ) ) {
            availableContainers.put( container.uniqueName, container );

            if ( !containersOnAdapter.containsKey( container.adapterId ) ) {
                containersOnAdapter.put( container.adapterId, ImmutableList.of( container.uniqueName ) );
            } else {
                List<String> containerNames = new ArrayList<>( containersOnAdapter.get( container.adapterId ) );
                containerNames.add( container.uniqueName );
                containersOnAdapter.put( container.adapterId, ImmutableList.copyOf( containerNames ) );
            }
        }
    }


    public Container createContainer( String uniqueName, int adapterId, Image image, int port ) {
        Container container = new Container( adapterId, uniqueName, image, Collections.singletonMap( image.internalPort, port ) );
        registerIfAbsent( container );
        return container;
    }


    private void start( Container container ) {
        registerIfAbsent( container );

        Ports bindings = new Ports();

        for ( Map.Entry<Integer, Integer> mapping : container.internalExternalPortMapping.entrySet() ) {
            bindings.bind( ExposedPort.tcp( mapping.getKey() ), Ports.Binding.bindPort( mapping.getValue() ) );
        }

        CreateContainerCmd cmd = client.createContainerCmd( container.type.getFullName() )
                .withName( container.uniqueName )
                .withExposedPorts( container.getExposedPorts() );

        Objects.requireNonNull( cmd.getHostConfig() ).withPortBindings( bindings );
        cmd.exec();

        client.startContainerCmd( container.uniqueName ).exec();

        container.setStatus( ContainerStatus.RUNNING );
    }


    public void download( Image image ) {
        PullImageResultCallback callback = new PullImageResultCallback();
        client.pullImageCmd( image.getFullName() ).exec( callback );

        // TODO: blocking for now, maybe change or show warning?
        try {
            callback.awaitCompletion();
        } catch ( InterruptedException e ) {
            throw new RuntimeException( "The downloading of the image  " + image.getFullName() + " failed." );
        }

        availableImages.add( image );
    }


    public void shutdownAll( int adapterId ) {
        containersOnAdapter.get( adapterId ).forEach( containerName -> availableContainers.get( containerName ).shutdown() );
    }


    private void shutdown( Container container ) {
        client.stopContainerCmd( container.uniqueName ).exec();
        client.removeContainerCmd( container.uniqueName ).exec();
    }


    /**
     * This enum unifies the name building and provides additional information of an image
     */
    public enum Image {
        MONGODB( "mongo", 27017 );

        @Getter
        private final String name;
        @Getter
        private String version;
        @Getter
        private final int internalPort;


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
        RUNNING,
        ERROR;
    }


    /**
     * The container is the main interaction instance for calling classes when interacting with Docker.
     * It holds all information for a specific Container
     */
    public static class Container {

        private final Image type;
        private final PullImageResultCallback callback;
        private final String uniqueName;
        private final Map<Integer, Integer> internalExternalPortMapping;
        private final int adapterId;
        private ContainerStatus status;


        private Container(
                int adapterId,
                String uniqueName,
                Image image,
                Map<Integer, Integer> internalExternalPortMapping
        ) {
            if ( !getInstance().checkIfUnique( uniqueName ) ) {
                throw new RuntimeException( "The name for the Docker container is already taken." );
            }
            this.adapterId = adapterId;
            this.type = image;
            this.uniqueName = uniqueName;
            this.internalExternalPortMapping = internalExternalPortMapping;
            this.callback = new PullImageResultCallback();
            this.status = ContainerStatus.INIT;
        }


        public Container start() {
            getInstance().start( this );

            return this;
        }


        public void shutdown() {
            getInstance().shutdown( this );
        }


        public List<ExposedPort> getExposedPorts() {
            return internalExternalPortMapping.values().stream().map( ExposedPort::tcp ).collect( Collectors.toList() );
        }


        private void setStatus( ContainerStatus status ) {
            this.status = status;
        }

    }


}

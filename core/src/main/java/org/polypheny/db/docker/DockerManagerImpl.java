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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.Exceptions.DockerNotRunningException;
import org.polypheny.db.docker.Exceptions.NameExistsException;
import org.polypheny.db.docker.Exceptions.PortExistsException;

// TODO DL: check if already existing containers in system etc.
// and move to core


/**
 * This class servers as a organization unit which controls all Docker containers in Polypheny.
 * While the callers can and should mostly interact with the underlying containers directly,
 * this instance is used to have a control layer, which allows to restore, start or shutdown multiple of
 * these instances at the same time.
 */
public class DockerManagerImpl extends DockerManager {

    private final DockerClient client;
    @Getter
    private final HashMap<String, Container> availableContainers = new HashMap<>();
    @Getter
    private final List<Image> availableImages = new ArrayList<>();
    @Getter
    private final HashMap<Integer, ImmutableList<String>> containersOnAdapter = new HashMap<>();

    // as Docker does not allow two containers with the same name or which expose the same port ( ports only for running containers )
    // we have to track them, so we can return correct messages to the user
    @Getter
    private final List<Integer> usedPorts = new ArrayList<>();
    @Getter
    private final List<String> usedNames = new ArrayList<>();


    DockerManagerImpl() {
        client = this.generateClient();

        // TODO DL: for now we throw here, when a more sophisticated frontend is present, maybe change
        if ( !hasDockerRunning() ) {
            throw new DockerNotRunningException();
        }

        client.listImagesCmd().exec().forEach( image -> {
            for ( String tag : image.getRepoTags() ) {
                String[] splits = tag.split( ":" );

                if ( splits[0].equals( Image.MONGODB.getName() ) ) {
                    availableImages.add( Image.MONGODB.setVersion( splits[1] ) );
                }
            }
        } );

        client.listContainersCmd().withShowAll( true ).exec().forEach( container -> {
            Arrays.stream( container.getPorts() ).forEach( containerPort -> {
                usedPorts.add( containerPort.getPublicPort() );
            } );
            // docker returns the names with a prefixed "/", so we remove it
            usedNames.addAll( Arrays.stream( container.getNames() ).map( cont -> cont.substring( 1 ) ).collect( Collectors.toList() ) );
        } );

    }


    private boolean hasDockerRunning() {
        // todo dl, better checking, exceptions for code flow is bad practice
        try {
            return null != client.infoCmd().exec();
        } catch ( Exception e ) {
            return false;
        }
    }


    private DockerClient generateClient() {
        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost( "tcp://" + RuntimeConfig.DOCKER_URL.getString() + ":" + RuntimeConfig.DOCKER_PORT.getInteger() )
                //.withDockerTlsVerify( true ) //TODO DL: use certificates
                //.withDockerCertPath(certPath)
                .build();

        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost( config.getDockerHost() )
                .sslConfig( config.getSSLConfig() )
                .build();

        return DockerClientImpl.getInstance( config, httpClient );
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


    @Override
    public Container createContainer( String uniqueName, int adapterId, Image image, int port, boolean isRestored ) {
        Container container = new Container( adapterId, uniqueName, image, Collections.singletonMap( image.internalPort, port ), isRestored );
        registerIfAbsent( container );
        return container;
    }


    @Override
    public Container createIfAbsent( String uniqueName, int adapterId, Image image, List<Integer> ports ) {
        if ( (!usedNames.contains( uniqueName ) && usedPorts.stream().noneMatch( ports::contains )) ) {
            // new container
            return createContainer( uniqueName, adapterId, image, ports.get( 0 ), false );
        } else if ( usedNames.contains( uniqueName ) /*&& usedPorts.containsAll( ports )*/ ) {
            // restored container
            return createContainer( uniqueName, adapterId, image, ports.get( 0 ), true );
        } else {
            throw new RuntimeException( "There was an error while restoring a Docker container." );
        }
    }


    void start( Container container ) {
        registerIfAbsent( container );

        if ( container.status == ContainerStatus.INIT ) {
            createNewContainer( container );
        }
        client.startContainerCmd( container.uniqueName ).exec();

        container.setStatus( ContainerStatus.RUNNING );
    }


    void tryRestart( Container container ) {
        // first we try to use the existing container
        if ( usedNames.contains( container.uniqueName ) ) {
            InspectContainerResponse containerInfo = client.inspectContainerCmd( "/" + container.uniqueName ).exec();
            ContainerState state = containerInfo.getState();
            if ( Objects.equals( state.getStatus(), "exited" ) ) {
                client.startContainerCmd( container.uniqueName ).exec();
            }
        } else {
            // our container does not exist, we have to create it from scratch
            start( container );
        }
    }


    private void createNewContainer( Container container ) {
        Ports bindings = new Ports();

        for ( Map.Entry<Integer, Integer> mapping : container.internalExternalPortMapping.entrySet() ) {
            // ExposedPort is exposed from container and Binding is port from outside
            bindings.bind( ExposedPort.tcp( mapping.getKey() ), Ports.Binding.bindPort( mapping.getValue() ) );
            if ( usedPorts.contains( mapping.getValue() ) ) {
                throw new PortExistsException();
            }
        }

        if ( usedNames.contains( container.uniqueName ) ) {
            throw new NameExistsException();
        }

        CreateContainerCmd cmd = client.createContainerCmd( container.type.getFullName() )
                .withName( container.uniqueName )
                .withExposedPorts( container.getExposedPorts() );

        Objects.requireNonNull( cmd.getHostConfig() ).withPortBindings( bindings );
        cmd.exec();

        // we add the name and the ports to our bookkeeping functions as all previous checks passed
        usedPorts.addAll( container.internalExternalPortMapping.values() );
        usedNames.add( container.uniqueName );
    }


    @Override
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


    @Override
    public void shutdownAll( int adapterId ) {
        containersOnAdapter.get( adapterId ).forEach( containerName -> availableContainers.get( containerName ).shutdown() );
    }


    void shutdown( Container container ) {
        client.stopContainerCmd( container.uniqueName ).exec();
        client.removeContainerCmd( container.uniqueName ).exec();
    }


    void stop( Container container ) {
        client.stopContainerCmd( container.uniqueName ).exec();
        container.setStatus( ContainerStatus.STOPPED );
    }


}

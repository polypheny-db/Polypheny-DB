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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;
import org.polypheny.db.docker.DockerManager.Container;
import org.polypheny.db.docker.DockerManager.ContainerStatus;
import org.polypheny.db.docker.DockerManager.Image;
import org.polypheny.db.util.Pair;

/**
 * These tests should mainly test the implementation of the
 * DockerManager and its functionality, the functionality of
 * the underlying java-docker library should not be tested
 */
public class DockerManagerTest {

    /**
     * We test if the DockerManager exposes the correct names to the callee
     */
    @Test
    public void usedNamesTest() {
        DockerManager manager = DockerManager.getInstance();

        List<String> uniqueNames = Arrays.asList( "test", "test1", "test2" );
        List<Integer> uniquePorts = Arrays.asList( 2302, 2301, 1201 );
        int adapterId = 1;

        Pair.zip( uniqueNames, uniquePorts ).forEach( namePortPairs -> {
            manager.initialize( namePortPairs.left, adapterId, Image.MONGODB, namePortPairs.right );
        } );
        assert (manager.getUsedNames().containsAll( uniqueNames ));
        assert (manager.getUsedPorts().containsAll( uniquePorts ));

        manager.destroyAll( adapterId );

        assert (!manager.getUsedNames().containsAll( uniqueNames ));
        assert (!manager.getUsedPorts().containsAll( uniquePorts ));
        assert (!manager.getContainersOnAdapter().containsKey( adapterId ) || manager.getContainersOnAdapter().get( adapterId ).stream().noneMatch( uniqueNames::contains ));

    }


    /**
     * We test if inserting multiple ports is correctly handled
     */
    @Test
    public void usedMultiplePortsTest() {
        DockerManager manager = DockerManager.getInstance();
        int adapterId = 1;

        String uniqueName = "test3";
        List<Integer> multiplePorts = Arrays.asList( 3210, 4929 );
        // for testing purpose we map the same ports internally to externally
        Map<Integer, Integer> internalExternalPorts = multiplePorts.stream().collect( Collectors.toMap( port -> port, port -> port ) );
        manager.initialize( uniqueName, adapterId, Image.MONGODB, internalExternalPorts );

        assert (manager.getUsedNames().contains( uniqueName ));
        assert (manager.getUsedPorts().containsAll( multiplePorts ));

        manager.destroyAll( adapterId );

    }


    /**
     * We test if the DockerManager correctly initializes a new container
     */
    @Test
    public void startNotExistsContainerTest() {
        String uniqueName = "testContainer";
        int usedPort = 5555;
        DockerManagerImpl managerLastSession = fakeLastSession( uniqueName, usedPort, true, false );

        //// new session has to handle already running container
        DockerManagerImpl managerThisSession = managerLastSession.generateNewSession();
        Container restoredContainer = managerThisSession.initialize( uniqueName, 1, Image.MONGODB, usedPort );
        managerThisSession.start( restoredContainer );

        assert (restoredContainer.status == ContainerStatus.RUNNING);
        assert (managerThisSession.getUsedNames().contains( uniqueName ));
        assert (managerThisSession.getUsedPorts().contains( usedPort ));

        managerThisSession.destroy( restoredContainer );

    }


    /**
     * Helper method which fakes a previous session, which was terminated and left the container in specified state
     *
     * @param uniqueName the name of the previous container
     * @param usedPort the used port of the previous container
     * @param doDestroy if the container was destroyed previously
     * @param doStop if the container was stopped
     * @return the managerImpl, which is used to fake a new session, but has to use the old client
     */
    private DockerManagerImpl fakeLastSession( String uniqueName, int usedPort, boolean doDestroy, boolean doStop ) {
        // so we can test the initialization process of the DockerManager,
        // when a container is already running
        // we use the impl here
        //// previous session left the container running

        DockerManagerImpl managerLastSession = new DockerManagerImpl();
        Container container = managerLastSession.initialize( uniqueName, 1, Image.MONGODB, usedPort );
        managerLastSession.start( container );

        assert (container.status == ContainerStatus.RUNNING);
        assert (managerLastSession.getUsedNames().contains( uniqueName ));
        assert (managerLastSession.getUsedPorts().contains( usedPort ));

        if ( doStop ) {
            managerLastSession.stop( container );

            assert (container.status == ContainerStatus.STOPPED);
            assert (managerLastSession.getUsedNames().contains( uniqueName ));
            assert (managerLastSession.getUsedPorts().contains( usedPort ));
        }

        if ( doDestroy ) {
            managerLastSession.destroy( container );

            assert (container.status == ContainerStatus.DESTROYED);
            assert (!managerLastSession.getUsedNames().contains( uniqueName ));
            assert (!managerLastSession.getUsedPorts().contains( usedPort ));
        }
        return managerLastSession;
    }


    /**
     * We test if an already running container on system start can correctly be restored
     */
    @Test
    public void runningContainerTest() {
        String uniqueName = "testContainer";
        int usedPort = 5555;
        DockerManagerImpl managerLastSession = fakeLastSession( uniqueName, usedPort, false, false );

        //// new session has to handle already running container
        DockerManagerImpl managerThisSession = managerLastSession.generateNewSession();
        Container restoredContainer = managerThisSession.initialize( uniqueName, 1, Image.MONGODB, usedPort );
        managerThisSession.start( restoredContainer );

        assert (restoredContainer.status == ContainerStatus.RUNNING);
        assert (managerThisSession.getUsedNames().contains( uniqueName ));
        assert (managerThisSession.getUsedPorts().contains( usedPort ));

        managerThisSession.destroy( restoredContainer );
    }


    /**
     * We test if a already existing container, which was stopped can be correctly restored
     */
    @Test
    public void stoppedContainerTest() {
        String uniqueName = "testContainer";
        int usedPort = 5555;
        DockerManagerImpl managerLastSession = fakeLastSession( uniqueName, usedPort, false, true );

        //// new session has to handle already running container
        DockerManagerImpl managerThisSession = managerLastSession.generateNewSession();
        Container restoredContainer = managerThisSession.initialize( uniqueName, 1, Image.MONGODB, usedPort );
        managerThisSession.start( restoredContainer );

        assert (restoredContainer.status == ContainerStatus.RUNNING);
        assert (managerThisSession.getUsedNames().contains( uniqueName ));
        assert (managerThisSession.getUsedPorts().contains( usedPort ));

        managerThisSession.destroy( restoredContainer );
    }

}

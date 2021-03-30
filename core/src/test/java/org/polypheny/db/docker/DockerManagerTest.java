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
import org.junit.Test;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.DockerManager.ContainerBuilder;
import org.polypheny.db.docker.DockerManager.Image;
import org.polypheny.db.util.Pair;

/**
 * These tests should mainly test the implementation of the
 * DockerManager and its functionality, the functionality of
 * the underlying java-docker library should not be tested
 */
public class DockerManagerTest {

    ConfigDocker config = RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, 0 );


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
            manager.initialize( new ContainerBuilder( adapterId, Image.MONGODB, namePortPairs.left, config.id ).withMappedPort( namePortPairs.right, namePortPairs.right ).build() );
        } );
        assert (manager.getUsedNames().containsAll( uniqueNames ));
        assert (manager.getUsedPorts().containsAll( uniquePorts ));

        manager.destroyAll( adapterId );

        assert (!manager.getUsedNames().containsAll( uniqueNames ));
        assert (!manager.getUsedPorts().containsAll( uniquePorts ));

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
        ContainerBuilder containerBuilder = new ContainerBuilder( adapterId, Image.MONGODB, uniqueName, config.id );
        multiplePorts.forEach( port -> containerBuilder.withMappedPort( port, port ) );
        manager.initialize( containerBuilder.build() );

        assert (manager.getUsedNames().contains( uniqueName ));
        assert (manager.getUsedPorts().containsAll( multiplePorts ));

        manager.destroyAll( adapterId );

    }

}

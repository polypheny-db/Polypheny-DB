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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.RuntimeConfig;

public class DockerManagerImpl extends DockerManager {

    private final Map<String, DockerInstance> dockerInstances = new HashMap<>();


    public DockerManagerImpl() {

        ConfigListener listener = new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                resetClients();
            }


            @Override
            public void restart( Config c ) {
                resetClients();
            }
        };
        resetClients();
        RuntimeConfig.DOCKER_URLS.addObserver( listener );
    }


    private void resetClients() {
        List<String> dockerUrls = RuntimeConfig.DOCKER_URLS.getStringList();
        // remove unused clients
        dockerInstances.keySet().stream().filter( url -> !dockerUrls.contains( url ) ).forEach( dockerInstances::remove );
        // add new clients
        dockerUrls.forEach( url -> {
            if ( !dockerInstances.containsKey( url ) ) {
                dockerInstances.put( url, new DockerInstance( url ) );
            }
        } );
    }


    @Override
    public Container initialize( Container container ) {
        dockerInstances.get( container.getDockerUrl() ).initialize( container );

        return container;
    }


    @Override
    public void start( Container container ) {
        dockerInstances.get( container.getDockerUrl() ).start( container );
    }


    @Override
    public void stop( Container container ) {
        dockerInstances.get( container.getDockerUrl() ).stop( container );
    }


    @Override
    public void destroy( Container container ) {
        dockerInstances.get( container.getDockerUrl() ).destroy( container );
    }


    @Override
    public void stopAll( int adapterId ) {
        dockerInstances.values().forEach( dockerInstance -> dockerInstance.stopAll( adapterId ) );
    }


    @Override
    public void destroyAll( int adapterId ) {
        dockerInstances.values().forEach( dockerInstance -> dockerInstance.destroyAll( adapterId ) );
    }

}

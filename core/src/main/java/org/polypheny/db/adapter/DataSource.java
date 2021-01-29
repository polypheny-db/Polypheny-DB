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

package org.polypheny.db.adapter;

import java.util.Map;
import lombok.Getter;

public abstract class DataSource extends Adapter {

    @Getter
    private final boolean dataReadOnly;


    protected DataSource( final int adapterId, final String uniqueName, final Map<String, String> settings, boolean dataReadOnly ) {
        super( adapterId, uniqueName, settings );
        this.dataReadOnly = dataReadOnly;
    }

    public Map<String, String> getCurrentSettings() {
        return settings;
    }

}

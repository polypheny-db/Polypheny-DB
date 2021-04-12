/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.transaction;

import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.information.InformationDuration;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.routing.Router;

public interface Statement {

    Transaction getTransaction();

    QueryProcessor getQueryProcessor();

    DataContext getDataContext();

    Context getPrepareContext();

    InformationDuration getDuration();

    Router getRouter();

    void unset();

}

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

import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptRule;

public class MongoConvetion extends Convention.Impl {

    public static final MongoConvetion INSTANCE = new MongoConvetion();


    public MongoConvetion() {
        super( "Mongo", MongoRel.class );
    }


    @Override
    public void register( RelOptPlanner planner ) {
        planner.addRule( MongoToEnumerableConverterRule.INSTANCE );
        for ( RelOptRule rule : MongoRules.RULES ) {
            planner.addRule( rule );
        }
    }

}

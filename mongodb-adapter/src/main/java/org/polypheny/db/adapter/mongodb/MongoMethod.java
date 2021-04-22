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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;


/**
 * Builtin methods in the MongoDB adapter.
 */
public enum MongoMethod {
    MONGO_QUERYABLE_FIND( MongoTable.MongoQueryable.class, "find", String.class, String.class, List.class ),
    MONGO_QUERYABLE_AGGREGATE( MongoTable.MongoQueryable.class, "aggregate", List.class, List.class ),
    MONGO_GET_RESULT( MongoTable.MongoQueryable.class, "getResults", List.class ),
    PREPARED_WRAPPER( MongoTable.MongoQueryable.class, "preparedWrapper", DataContext.class ),
    PREPARED_EXECUTE( MongoTable.MongoQueryable.class, "preparedExecute", List.class, Map.class, Map.class, Map.class, Map.class, Map.class );

    public final Method method;

    public static final ImmutableMap<Method, MongoMethod> MAP;


    static {
        final ImmutableMap.Builder<Method, MongoMethod> builder = ImmutableMap.builder();
        for ( MongoMethod value : MongoMethod.values() ) {
            builder.put( value.method, value );
        }
        MAP = builder.build();
    }


    MongoMethod( Class clazz, String methodName, Class... argumentTypes ) {
        this.method = Types.lookupMethod( clazz, methodName, argumentTypes );
    }
}

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

package org.polypheny.db.sql.validate;


import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.util.PolyphenyDbValidatorException;

// NOTE:  This class gets compiled independently of everything else so that resource generation can use reflection.  That means it must have no dependencies on other Polypheny-DB code.


/**
 * Exception thrown while validating a SQL statement.
 *
 * Unlike {@link PolyphenyDbException}, this is a checked exception, which reminds code authors to wrap it in another exception containing the line/column context.
 */
@Slf4j
public class SqlValidatorException extends Exception implements PolyphenyDbValidatorException {

    static final long serialVersionUID = -831683113957131387L;


    /**
     * Creates a new SqlValidatorException object.
     *
     * @param message error message
     * @param cause underlying cause
     */
    public SqlValidatorException( String message, Throwable cause ) {
        super( message, cause );

        // TODO: see note in PolyphenyDbException constructor
        log.trace( "SqlValidatorException", this );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            log.error( toString() );
        }
    }
}


/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.runtime;


import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import lombok.extern.slf4j.Slf4j;

// NOTE:  This class gets compiled independently of everything else so that resource generation can use reflection.  That means it must have no dependencies on other Polypheny-DB code.


/**
 * Base class for all exceptions originating from Farrago.
 *
 * @see PolyphenyDbContextException
 */
@Slf4j
public class PolyphenyDbException extends RuntimeException {

    private static final long serialVersionUID = -1314522633397794178L;


    /**
     * Creates a new PolyphenyDbException object.
     *
     * @param message error message
     * @param cause underlying cause
     */
    public PolyphenyDbException( String message, Throwable cause ) {
        super( message, cause );

        // TODO: Force the caller to pass in a Logger as a trace argument for better context.  Need to extend ResGen for this.
        log.trace( "PolyphenyDbException", this );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            log.error( toString() );
        }
    }


    /**
     * Creates a new PolyphenyDbException object.
     *
     * @param message error message
     */
    public PolyphenyDbException( String message ) {
        super( message );

        // TODO: Force the caller to pass in a Logger as a trace argument for better context.  Need to extend ResGen for this.
        log.trace( "PolyphenyDbException", this );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            log.error( toString() );
        }
    }
}


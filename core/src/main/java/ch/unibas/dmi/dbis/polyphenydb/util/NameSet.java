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

package ch.unibas.dmi.dbis.polyphenydb.util;


import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;


/**
 * Set of names that can be accessed with and without case sensitivity.
 */
public class NameSet {

    public static final Comparator<String> COMPARATOR = CaseInsensitiveComparator.COMPARATOR;

    private static final Object DUMMY = new Object();

    private final NameMap<Object> names;


    /**
     * Creates a NameSet based on an existing set.
     */
    private NameSet( NameMap<Object> names ) {
        this.names = names;
    }


    /**
     * Creates a NameSet, initially empty.
     */
    public NameSet() {
        this( new NameMap<>() );
    }


    /**
     * Creates a NameSet that is an immutable copy of a given collection.
     */
    public static NameSet immutableCopyOf( Set<String> names ) {
        return new NameSet( NameMap.immutableCopyOf( Maps.asMap( names, ( k ) -> DUMMY ) ) );
    }


    @Override
    public String toString() {
        return names.map().keySet().toString();
    }


    @Override
    public int hashCode() {
        return names.hashCode();
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof NameSet
                && names.equals( ((NameSet) obj).names );
    }


    public void add( String name ) {
        names.put( name, DUMMY );
    }


    /**
     * Returns an iterable over all the entries in the set that match the given name. If case-sensitive, that iterable will have 0 or 1 elements; if
     * case-insensitive, it may have 0 or more.
     */
    public Collection<String> range( String name, boolean caseSensitive ) {
        return names.range( name, caseSensitive ).keySet();
    }


    /**
     * Returns whether this set contains the given name, with a given case-sensitivity.
     */
    public boolean contains( String name, boolean caseSensitive ) {
        return names.containsKey( name, caseSensitive );
    }


    /**
     * Returns the contents as an iterable.
     */
    public Iterable<String> iterable() {
        return Collections.unmodifiableSet( names.map().keySet() );
    }
}


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

package ch.unibas.dmi.dbis.polyphenydb.util.graph;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


/**
 * Iterates over the vertices in a directed graph in depth-first order.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
 */
public class DepthFirstIterator<V, E extends DefaultEdge> implements Iterator<V> {

    private final Iterator<V> iterator;


    public DepthFirstIterator( DirectedGraph<V, E> graph, V start ) {
        // Dumb implementation that builds the list first.
        iterator = buildList( graph, start ).iterator();
    }


    private static <V, E extends DefaultEdge> List<V> buildList( DirectedGraph<V, E> graph, V start ) {
        final List<V> list = new ArrayList<>();
        buildListRecurse( list, new HashSet<>(), graph, start );
        return list;
    }


    /**
     * Creates an iterable over the vertices in the given graph in a depth-first iteration order.
     */
    public static <V, E extends DefaultEdge> Iterable<V> of( DirectedGraph<V, E> graph, V start ) {
        // Doesn't actually return a DepthFirstIterator, but a list with the same contents, which is more efficient.
        return buildList( graph, start );
    }


    /**
     * Populates a collection with the nodes reachable from a given node.
     */
    public static <V, E extends DefaultEdge> void reachable( Collection<V> list, final DirectedGraph<V, E> graph, final V start ) {
        buildListRecurse( list, new HashSet<>(), graph, start );
    }


    private static <V, E extends DefaultEdge> void buildListRecurse( Collection<V> list, Set<V> activeVertices, DirectedGraph<V, E> graph, V start ) {
        if ( !activeVertices.add( start ) ) {
            return;
        }
        list.add( start );
        List<E> edges = graph.getOutwardEdges( start );
        for ( E edge : edges ) {
            //noinspection unchecked
            buildListRecurse( list, activeVertices, graph, (V) edge.target );
        }
        activeVertices.remove( start );
    }


    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }


    @Override
    public V next() {
        return iterator.next();
    }


    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

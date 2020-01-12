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

package ch.unibas.dmi.dbis.polyphenydb.test;


import static org.junit.Assert.assertEquals;

import ch.unibas.dmi.dbis.polyphenydb.util.Sources;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.function.Consumer;
import org.junit.Ignore;


/**
 * Tests for the {@code ch.unibas.dmi.dbis.polyphenydb.adapter.pig} package.
 */
@Ignore
public class PigAdapterTest extends AbstractPigTest {

    // Undo the %20 replacement of a space by URL
    public static final ImmutableMap<String, String> MODEL = ImmutableMap.of( "model", Sources.of( PigAdapterTest.class.getResource( "/model.json" ) ).file().getAbsolutePath() );


    // TODO MV: enable
//    @Test
//    public void testScanAndFilter() throws Exception {
//        PolyphenyDbAssert.that()
//                .with( MODEL )
//                .query( "select * from \"t\" where \"tc0\" > 'abc'" )
//                .explainContains( "PigToEnumerableConverter\n"
//                        + "  PigFilter(condition=[>($0, 'abc')])\n"
//                        + "    PigTableScan(table=[[PIG, t]])" )
//                .runs()
//                .queryContains(
//                        pigScriptChecker( "t = LOAD '"
//                                + getFullPathForTestDataFile( "data.txt" )
//                                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
//                                + "t = FILTER t BY (tc0 > 'abc');" ) );
//    }
//
//
//    @Test
//    public void testImplWithMultipleFilters() {
//        PolyphenyDbAssert.that()
//                .with( MODEL )
//                .query( "select * from \"t\" where \"tc0\" > 'abc' and \"tc1\" = '3'" )
//                .explainContains( "PigToEnumerableConverter\n"
//                        + "  PigFilter(condition=[AND(>($0, 'abc'), =($1, '3'))])\n"
//                        + "    PigTableScan(table=[[PIG, t]])" )
//                .runs()
//                .queryContains(
//                        pigScriptChecker( "t = LOAD '"
//                                + getFullPathForTestDataFile( "data.txt" )
//                                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
//                                + "t = FILTER t BY (tc0 > 'abc') AND (tc1 == '3');" ) );
//    }
//
//
//    @Test
//    public void testImplWithGroupByAndCount() {
//        PolyphenyDbAssert.that()
//                .with( MODEL )
//                .query( "select count(\"tc1\") c from \"t\" group by \"tc0\"" )
//                .explainContains( "PigToEnumerableConverter\n"
//                        + "    PigAggregate(group=[{0}], C=[COUNT($1)])\n"
//                        + "      PigTableScan(table=[[PIG, t]])" )
//                .runs()
//                .queryContains(
//                        pigScriptChecker( "t = LOAD '"
//                                + getFullPathForTestDataFile( "data.txt" )
//                                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
//                                + "t = GROUP t BY (tc0);\n"
//                                + "t = FOREACH t {\n"
//                                + "  GENERATE group AS tc0, COUNT(t.tc1) AS C;\n"
//                                + "};" ) );
//    }
//
//
//    @Test
//    public void testImplWithCountWithoutGroupBy() {
//        PolyphenyDbAssert.that()
//                .with( MODEL )
//                .query( "select count(\"tc0\") c from \"t\"" )
//                .explainContains( "PigToEnumerableConverter\n"
//                        + "  PigAggregate(group=[{}], C=[COUNT($0)])\n"
//                        + "    PigTableScan(table=[[PIG, t]])" )
//                .runs()
//                .queryContains(
//                        pigScriptChecker( "t = LOAD '"
//                                + getFullPathForTestDataFile( "data.txt" )
//                                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
//                                + "t = GROUP t ALL;\n"
//                                + "t = FOREACH t {\n"
//                                + "  GENERATE COUNT(t.tc0) AS C;\n"
//                                + "};" ) );
//    }
//
//
//    @Test
//    public void testImplWithGroupByMultipleFields() {
//        PolyphenyDbAssert.that()
//                .with( MODEL )
//                .query( "select * from \"t\" group by \"tc1\", \"tc0\"" )
//                .explainContains( "PigToEnumerableConverter\n"
//                        + "  PigAggregate(group=[{0, 1}])\n"
//                        + "    PigTableScan(table=[[PIG, t]])" )
//                .runs()
//                .queryContains(
//                        pigScriptChecker( "t = LOAD '"
//                                + getFullPathForTestDataFile( "data.txt" )
//                                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
//                                + "t = GROUP t BY (tc0, tc1);\n"
//                                + "t = FOREACH t {\n"
//                                + "  GENERATE group.tc0 AS tc0, group.tc1 AS tc1;\n"
//                                + "};" ) );
//    }
//
//
//    @Test
//    public void testImplWithGroupByCountDistinct() {
//        PolyphenyDbAssert.that()
//                .with( MODEL )
//                .query( "select count(distinct \"tc0\") c from \"t\" group by \"tc1\"" )
//                .explainContains( "PigToEnumerableConverter\n"
//                        + "    PigAggregate(group=[{1}], C=[COUNT(DISTINCT $0)])\n"
//                        + "      PigTableScan(table=[[PIG, t]])" )
//                .runs()
//                .queryContains(
//                        pigScriptChecker( "t = LOAD '"
//                                + getFullPathForTestDataFile( "data.txt" )
//                                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
//                                + "t = GROUP t BY (tc1);\n"
//                                + "t = FOREACH t {\n"
//                                + "  tc0_DISTINCT = DISTINCT t.tc0;\n"
//                                + "  GENERATE group AS tc1, COUNT(tc0_DISTINCT) AS C;\n"
//                                + "};" ) );
//    }
//
//
//    @Test
//    public void testImplWithJoin() throws Exception {
//        PolyphenyDbAssert.that()
//                .with( MODEL )
//                .query( "select * from \"t\" join \"s\" on \"tc1\"=\"sc0\"" )
//                .explainContains( "PigToEnumerableConverter\n"
//                        + "  PigJoin(condition=[=($1, $2)], joinType=[inner])\n"
//                        + "    PigTableScan(table=[[PIG, t]])\n"
//                        + "    PigTableScan(table=[[PIG, s]])" )
//                .runs()
//                .queryContains(
//                        pigScriptChecker( "t = LOAD '"
//                                + getFullPathForTestDataFile( "data.txt" )
//                                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
//                                + "s = LOAD '" + getFullPathForTestDataFile( "data2.txt" )
//                                + "' USING PigStorage() AS (sc0:chararray, sc1:chararray);\n"
//                                + "t = JOIN t BY tc1 , s BY sc0;" ) );
//    }


    /**
     * Returns a function that checks that a particular Pig Latin scriptis generated to implement a query.
     */
    @SuppressWarnings("rawtypes")
    private static Consumer<List> pigScriptChecker( final String... strings ) {
        return actual -> {
            String actualArray =
                    actual == null || actual.isEmpty()
                            ? null
                            : (String) actual.get( 0 );
            assertEquals( "expected Pig script not found", strings[0], actualArray );
        };
    }
}

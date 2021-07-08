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

package org.polypheny.db.sql.fun;



import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@Slf4j
public class TrigonometricFunctionsTest {


    @BeforeClass
    public static void start() throws SQLException {
        TestHelper.getInstance();
        addTestData();
    }


    private static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE trigotestdecimal( AngleinDegree INTEGER NOT NULL,AngleinRadian DECIMAL(6,4), PRIMARY KEY (AngleinDegree) )" );
                statement.executeUpdate( "INSERT INTO trigotestdecimal VALUES (0, 0)" );
                statement.executeUpdate( "INSERT INTO trigotestdecimal VALUES (30, 0.52)" );
                statement.executeUpdate( "INSERT INTO trigotestdecimal  VALUES (45, 0.61)" );

                statement.executeUpdate( "CREATE TABLE trigotestdouble( AngleinDegree INTEGER NOT NULL, AngleinRadian DOUBLE , PRIMARY KEY (AngleinDegree) )" );
                statement.executeUpdate( "INSERT INTO trigotestdouble VALUES (0, 0)" );
                statement.executeUpdate( "INSERT INTO trigotestdouble VALUES (30, 0.52)" );
                statement.executeUpdate( "INSERT INTO trigotestdouble VALUES (45, 0.61)" );

               statement.executeUpdate( "CREATE TABLE trigotestinteger( AngleinDegree INTEGER NOT NULL, AngleinRadian INTEGER, PRIMARY KEY (AngleinDegree) )" );
               statement.executeUpdate( "INSERT INTO trigotestinteger VALUES (0,  0)" );
               statement.executeUpdate( "INSERT INTO trigotestinteger VALUES (58, 1)" );


                connection.commit();
            }
        }
    }


    @AfterClass
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE trigotestdecimal" );
                statement.executeUpdate( "DROP TABLE trigotestdouble" );
                statement.executeUpdate( "DROP TABLE trigotestinteger" );
            }
        }
    }

    // --------------- Tests ---------------


    @Test
    public void Sine() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {


                //For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0},
                        new Object[]{ 30, 0.49688013784373675},
                        new Object[]{ 45, 0.5728674601004813}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT Angleindegree, SIN(AngleinRadian) FROM trigotestdecimal"),
                        expectedResult
                );

                //For Double
                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{ 0, 0.0},
                        new Object[]{ 30, 0.49688},
                        new Object[]{ 45, 0.572867}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(SIN(AngleinRadian),6) FROM trigotestdouble"),
                        expectedResult1
                );



                //For Integer
                List<Object[]> expectedResult2 = ImmutableList.of(
                        new Object[]{ 0, 0.0},
                        new Object[]{ 58, 0.841471}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree,SIN(AngleinRadian) FROM trigotestinteger"),
                        expectedResult2
                );



            }
        }

    }


    @Test
    public void Cos() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {

                //For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1.0},
                        new Object[]{ 30, 0.8678191796776499},
                        new Object[]{ 45, 0.8196480178454796}
                );


                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, COS(AngleinRadian) FROM trigotestdecimal" ),
                        expectedResult
                );


                //For Double
                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{ 0, 1.0},
                        new Object[]{ 30, 0.867819},
                        new Object[]{ 45, 0.819648}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(COS(AngleinRadian),6) FROM trigotestdouble" ),
                        expectedResult1
                );

                //For Integer
                List<Object[]> expectedResult2 = ImmutableList.of(
                        new Object[]{ 0, 1.0},
                        new Object[]{ 58, 0.540302}

                );


                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, COS(AngleinRadian) FROM trigotestinteger" ),
                        expectedResult2
                );


            }

        }

    }

    @Test
    public void Tan() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {

                //For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0},
                        new Object[]{ 30, 0.5725618302516684},
                        new Object[]{ 45, 0.698918862277391}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, TAN(AngleinRadian) FROM trigotestdecimal" ),
                        expectedResult
                );


                //For Double
                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{ 0, 0.0},
                        new Object[]{ 30, 0.572562},
                        new Object[]{ 45, 0.698919}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(TAN(AngleinRadian),6) FROM trigotestdouble" ),
                        expectedResult1
                );


                //For Integer
                List<Object[]> expectedResult2 = ImmutableList.of(
                        new Object[]{ 0, 0.0},
                        new Object[]{ 58, 1.557408}


                );


                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(TAN(AngleinRadian),6) FROM trigotestinteger" ),
                        expectedResult2
                );

            }




        }

    }
    @Test
    public void Asin() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {

                //For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0},
                        new Object[]{ 30, 0.5468509506959441},
                        new Object[]{ 45, 0.6560605909249226}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ASIN(AngleinRadian) FROM trigotestdecimal" ),
                        expectedResult
                );


                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{ 0, 0.0},
                        new Object[]{ 30, 0.546851},
                        new Object[]{ 45, 0.656061}

                );

                //For Double
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ASIN(AngleinRadian),6) FROM trigotestdouble" ),
                        expectedResult1
                );

                List<Object[]> expectedResult2 = ImmutableList.of(
                        new Object[]{ 0, 0.0},
                        new Object[]{ 58, 1.570796}


                );

                //For Integer
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ASIN(AngleinRadian),6) FROM trigotestinteger" ),
                        expectedResult2
                );

            }

        }

    }
    @Test
    public void Acos() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {

                //For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 1.5707963267948966},
                        new Object[]{ 30, 1.0239453760989525},
                        new Object[]{ 45, 0.914735735869974}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ACOS(AngleinRadian),6) FROM trigotestdecimal" ),
                        expectedResult
                );

                //For Double
                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{ 0, 1},
                        new Object[]{ 30, 0.867819},
                        new Object[]{ 45, 0.819648}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ACOS(AngleinRadian),6) FROM trigotestdouble" ),
                        expectedResult
                );


                //For Integer
                List<Object[]> expectedResult2 = ImmutableList.of(
                        new Object[]{ 0,  1.570796},
                        new Object[]{ 58, 0.0}


                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ACOS(AngleinRadian),6) FROM trigotestinteger" ),
                        expectedResult2
                );




            }

        }

    }
    @Test
    public void Atan() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {

                //For Decimal
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0, 0.0},
                        new Object[]{ 30, 0.4795192919925962},
                        new Object[]{ 45, 0.5477400137159024}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ATAN(AngleinRadian) FROM trigotestdecimal" ),
                        expectedResult
                );

                //For Double
                List<Object[]> expectedResult1 = ImmutableList.of(
                        new Object[]{ 0, 0.0},
                        new Object[]{ 30, 0.479519},
                        new Object[]{ 45, 0.54774}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ATAN(AngleinRadian),6) FROM trigotestdouble" ),
                        expectedResult1
                );


                //For Integer
                List<Object[]> expectedResult2 = ImmutableList.of(
                        new Object[]{ 0, 0.0},
                        new Object[]{ 58, 0.785398}

                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT AngleinDegree, ROUND(ATAN(AngleinRadian),6) FROM trigotestinteger" ),
                        expectedResult2
                );

            }


        }

    }



}

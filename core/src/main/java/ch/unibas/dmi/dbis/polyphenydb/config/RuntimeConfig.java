/*
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
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.config;


import java.math.BigDecimal;


public enum RuntimeConfig {

    APPROXIMATE_DISTINCT_COUNT(
            "runtime/approximateDistinctCount",
            "Whether approximate results from \"COUNT(DISTINCT ...)\" aggregate functions are acceptable.",
            false,
            ConfigType.BOOLEAN
    ), // Druid

    APPROXIMATE_TOP_N(
            "runtime/approximateTopN",
            "Whether approximate results from \"Top N\" queries (\"ORDER BY aggFun DESC LIMIT n\") are acceptable.",
            false,
            ConfigType.BOOLEAN
    ), // Druid

    APPROXIMATE_DECIMAL(
            "runtime/approximateDecimal",
            "Whether approximate results from aggregate functions on DECIMAL types are acceptable.",
            false,
            ConfigType.BOOLEAN
    ), // Druid

    NULL_EQUAL_TO_EMPTY(
            "runtime/nullEqualToEmpty",
            "Whether to treat empty strings as null for Druid Adapter.",
            true,
            ConfigType.BOOLEAN
    ), // Druid

    AUTO_TEMP(
            "runtime/autoTemp",
            "Whether to store query results in temporary tables.",
            false,
            ConfigType.BOOLEAN
    ),

    CASE_SENSITIVE(
            "runtime/caseSensitive",
            "Whether identifiers are matched case-sensitively.",
            false,
            ConfigType.BOOLEAN
    ),

    SPARK_ENGINE(
            "runtime/sparkEngine",
            "Specifies whether Spark should be used as the engine for processing that cannot be pushed to the source system. If false, Polypheny-DB generates code that implements the Enumerable interface.",
            false,
            ConfigType.BOOLEAN
    ),

    JDBC_PORT(
            "runtime/jdbcPort",
            "The port on which the JDBC server should listen.",
            20591,
            ConfigType.INTEGER
    ),

    CONFIG_SERVER_PORT(
            "runtime/configServerPort",
            "The port on which the config server should listen.",
            8081,
            ConfigType.INTEGER
    ),

    INFORMATION_SERVER_PORT(
            "runtime/informationServerPort",
            "The port on which the information server should listen.",
            8082,
            ConfigType.INTEGER
    ),

    WEBUI_SERVER_PORT(
            "runtime/webuiServerPort",
            "The port on which the web ui server should listen.",
            8080,
            ConfigType.INTEGER
    ),

    REL_WRITER_INSERT_FIELD_NAMES( "runtime/relWriterInsertFieldName",
            "If the rel writer should add the field names in brackets behind the ordinals in when printing query plans.",
            false,
            ConfigType.BOOLEAN ),

    QUERY_TIMEOUT( "runtime/queryTimeout",
            "Time after which queries are aborted. 0 means infinite.",
            0,
            ConfigType.INTEGER ),

    DEFAULT_INDEX_TYPE( "runtime/defaultIndexType",
            "Index type to use if no type is specified in the create index statement.",
            1,
            ConfigType.INTEGER ),

    DEFAULT_COLLATION( "runtime/defaultCollation",
            "Collation to use if no collation is specified",
            2,
            ConfigType.INTEGER ),

    GENERATED_NAME_PREFIX( "runtime/generatedNamePrefix",
            "Prefix for generated index, foreign key and constraint names.",
            "auto",
            ConfigType.STRING ),

    ADD_DEFAULT_VALUES_IN_INSERTS( "processing/addDefaultValuesInInserts",
            "Reorder columns and add default values in insert statements.",
            true,
            ConfigType.BOOLEAN,
            "parsingGroup" ),

    TRIM_UNUSED_FIELDS( "processing/trimUnusedFields",
            "Walks over a tree of relational expressions, replacing each RelNode with a 'slimmed down' relational expression that projects only the columns required by its consumer.",
            true,
            ConfigType.BOOLEAN,
            "planningGroup" ),

    DEBUG( "runtime/debug",
            "Print debugging output.",
            false,
            ConfigType.BOOLEAN ),

    JOIN_COMMUTE( "runtime/joinCommute",
            "Commute joins in planner.",
            false,
            ConfigType.BOOLEAN ),

    TWO_PC_MODE( "runtime/twoPcMode",
            "Commute joins in planner.",
            false,
            ConfigType.BOOLEAN,
            "runtimeGroup" );


    private final String key;
    private final String description;

    private final ConfigManager configManager = ConfigManager.getInstance();


    static {
        final ConfigManager configManager = ConfigManager.getInstance();

        final WebUiPage processingPage = new WebUiPage( "processingPage", "Query Processing", "Settings influencing the query processing." );
        final WebUiGroup planningGroup = new WebUiGroup( "planningGroup", processingPage.getId() );
        final WebUiGroup parsingGroup = new WebUiGroup( "parsingGroup", processingPage.getId() );

        final WebUiPage runtimePage = new WebUiPage( "runtimePage", "Runtime Settings", "Core Settings" );
        final WebUiGroup runtimeGroup = new WebUiGroup( "runtimeGroup", runtimePage.getId() );

        configManager.registerWebUiPage( processingPage );
        configManager.registerWebUiPage( runtimePage );

        configManager.registerWebUiGroup( parsingGroup );
        configManager.registerWebUiGroup( planningGroup );
        configManager.registerWebUiGroup( runtimeGroup );
    }


    RuntimeConfig( final String key, final String description, final Object defaultValue, final ConfigType configType ) {
        this( key, description, defaultValue, configType, null );
    }


    RuntimeConfig( final String key, final String description, final Object defaultValue, final ConfigType configType, final String webUiGroup ) {
        this.key = key;
        this.description = description;

        final Config config;
        switch ( configType ) {
            case BOOLEAN:
                config = new ConfigBoolean( key, description, (boolean) defaultValue );
                break;

            case DECIMAL:
                config = new ConfigDecimal( key, description, (BigDecimal) defaultValue );
                break;

            case DOUBLE:
                config = new ConfigDouble( key, description, (double) defaultValue );
                break;

            case INTEGER:
                config = new ConfigInteger( key, description, (int) defaultValue );
                break;

            case LONG:
                config = new ConfigLong( key, description, (long) defaultValue );
                break;

            case STRING:
                config = new ConfigString( key, description, (String) defaultValue );
                break;

            case BOOLEAN_TABLE:
                config = new ConfigTable( key, (boolean[][]) defaultValue );
                break;

            case DECIMAL_TABLE:
                config = new ConfigTable( key, (BigDecimal[][]) defaultValue );
                break;

            case DOUBLE_TABLE:
                config = new ConfigTable( key, (double[][]) defaultValue );
                break;

            case INTEGER_TABLE:
                config = new ConfigTable( key, (int[][]) defaultValue );
                break;

            case LONG_TABLE:
                config = new ConfigTable( key, (long[][]) defaultValue );
                break;

            case STRING_TABLE:
                config = new ConfigTable( key, (String[][]) defaultValue );
                break;

            case BOOLEAN_ARRAY:
                config = new ConfigArray( key, (boolean[]) defaultValue );
                break;

            case DECIMAL_ARRAY:
                config = new ConfigArray( key, (BigDecimal[]) defaultValue );
                break;

            case DOUBLE_ARRAY:
                config = new ConfigArray( key, (double[]) defaultValue );
                break;

            case INTEGER_ARRAY:
                config = new ConfigArray( key, (int[]) defaultValue );
                break;

            case LONG_ARRAY:
                config = new ConfigArray( key, (long[]) defaultValue );
                break;

            case STRING_ARRAY:
                config = new ConfigArray( key, (String[]) defaultValue );
                break;

            default:
                throw new RuntimeException( "Unknown config type: " + configType.name() );
        }
        configManager.registerConfig( config );
        if ( webUiGroup != null ) {
            config.withUi( webUiGroup );
        }
    }


    public boolean getBoolean() {
        return configManager.getConfig( key ).getBoolean();
    }


    public BigDecimal getDecimal() {
        return configManager.getConfig( key ).getDecimal();
    }


    public double getDouble() {
        return configManager.getConfig( key ).getDouble();
    }


    public int getInteger() {
        return configManager.getConfig( key ).getInt();
    }


    public long getLong() {
        return configManager.getConfig( key ).getLong();
    }


    public String getString() {
        return configManager.getConfig( key ).getString();
    }

    // TODO: Add methods for array and table


    public void setBoolean( final boolean value ) {
        configManager.getConfig( key ).setBoolean( value );
    }


    public void setDecimal( final BigDecimal value ) {
        configManager.getConfig( key ).setDecimal( value );
    }


    public void setDouble( final double value ) {
        configManager.getConfig( key ).setDouble( value );
    }


    public void setInteger( final int value ) {
        configManager.getConfig( key ).setInt( value );
    }


    public void setLong( final long value ) {
        configManager.getConfig( key ).setLong( value );
    }


    public void setString( final String value ) {
        configManager.getConfig( key ).setString( value );
    }


    public enum ConfigType {
        BOOLEAN, DECIMAL, DOUBLE, INTEGER, LONG, STRING, BOOLEAN_TABLE, DECIMAL_TABLE, DOUBLE_TABLE, INTEGER_TABLE, LONG_TABLE, STRING_TABLE, BOOLEAN_ARRAY, DECIMAL_ARRAY, DOUBLE_ARRAY, INTEGER_ARRAY, LONG_ARRAY, STRING_ARRAY
    }

}

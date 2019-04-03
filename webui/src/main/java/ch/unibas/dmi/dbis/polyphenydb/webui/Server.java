/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
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

package ch.unibas.dmi.dbis.polyphenydb.webui;

import static spark.Spark.*;

import ch.unibas.dmi.dbis.polyphenydb.config.*;
import com.google.gson.Gson;
import java.util.Map;


/** RESTful server for the WebUis */
public class Server {

    static {
        ConfigManager.getInstance().registerConfig( new ConfigString( "server.test", "just for testing" ).requiresRestart() );
    }

    public Server() {

        port(8081);

        //needs to be called before route mapping!
        webSockets();

        enableCORS();

        configRoutes();

        demoData();

    }

    public static void main(String[] args) {
        new Server();
        System.out.println("server running..");
    }

    private void webSockets () {
        //Websockets need to be defined before the post/get requests
        webSocket("/echo", WebUiWebsocket.class);
    }

    /** many routes just for testing
     * route getPage: get a WebUiPage as JSON (with all its groups and configs */
    private void configRoutes () {
        String type = "application/json";
        Gson gson = new Gson();
        ConfigManager cm = ConfigManager.getInstance();

        // add a new config
        post("/newConfig", (req, res) -> {
            //res.type(type);
            Config c = gson.fromJson( req.body(),Config.class );
            System.out.println(c.toString());
            cm.registerConfig( c );
            return gson.toJson( c );
        });

        //add a WebUiGroup
        post("/newWebUiGroup", (req, res) -> {
            //res.type(type);
            WebUiGroup g = gson.fromJson( req.body(), WebUiGroup.class );
            System.out.println(g.toString());
            cm.addUiGroup( g );
            return gson.toJson( g );
        });

        //add a WebUiPage
        post("/newWebUiPage", (req, res) -> {
            //res.type(type);
            WebUiPage p = gson.fromJson( req.body(), WebUiPage.class );
            System.out.println(p.toString());
            cm.addUiPage( p );
            return gson.toJson( p );
        });

        get("/getPageList", ( req, res ) -> cm.getPageList());

        //get Ui of certain page
        post("/getPage", (req, res) -> {
            //input: req: {pageId: 123}
            try{
                System.out.println("get page "+req.body());
                int pageId = Integer.parseInt( req.body() );
                if( pageId == 0 ){
                    //todo load page list or so.
                    return "";
                } else{
                    return cm.getPage( pageId );
                }
            } catch ( Exception e ){
                //if input not number or page does not exist
                return "";
            }
        });

        //save changes from WebUi
        post("/updateConfigs", (req, res) -> {
            System.out.println(req.body());
            Map<String, Object> changes = gson.fromJson(req.body(), Map.class);
            for (Map.Entry<String, Object> entry : changes.entrySet()) {
                //todo give feedback if config does not exists
                cm.setConfigValue( entry.getKey(), entry.getValue() );
            }
            return "{\"success\":1}";
        });
    }

    // https://gist.github.com/saeidzebardast/e375b7d17be3e0f4dddf
    /** to avoid the CORS problem, when the Server receives requests from the WebUi */
    private static void enableCORS() {
        staticFiles.header("Access-Control-Allow-Origin", "*");

        options("/*", (req, res) -> {
            String accessControlRequestHeaders = req.headers("Access-Control-Request-Headers");
            if (accessControlRequestHeaders != null) {
                res.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
            }

            String accessControlRequestMethod = req.headers("Access-Control-Request-Method");
            if (accessControlRequestMethod != null) {
                res.header("Access-Control-Allow-Methods", accessControlRequestMethod);
            }

            return "OK";
        });

        before((req, res) -> {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Access-Control-Allow-Headers", "*");
            res.type("application/json");
        });
    }

    /** just for testing */
    private static void demoData() {
        WebUiPage p = new WebUiPage( 1, "page 1", "page 1 descr." );
        WebUiPage p2 = new WebUiPage( 2, "page 2", "page 2 description." ).withIcon( "fa fa-table" );
        WebUiGroup g1 = new WebUiGroup( 1, 1 ).withTitle( "group1" ).withDescription( "description of group1" );
        WebUiGroup g2 = new WebUiGroup( 2, 2 ).withDescription( "group2" );
        Config c1 = new ConfigString("server.text.1").withUi( 1, WebUiFormType.TEXT ).withValidation( WebUiValidator.REQUIRED );
        Config c2 = new ConfigString("server.email.2").withUi( 1, WebUiFormType.TEXT ).withValidation( WebUiValidator.REQUIRED, WebUiValidator.EMAIL );

        ConfigInteger c3 = new ConfigInteger( "server.number" );
        ConfigInteger c4 = new ConfigInteger( "server.number" ).withUi( 2, WebUiFormType.NUMBER );
        ConfigNumber c5 = new ConfigNumber( "server.number.2" ).withUi( 2, WebUiFormType.NUMBER );

        ConfigManager cm = ConfigManager.getInstance();

        //inserting configs before groups and pages are existing
        cm.registerConfig( c1 );
        cm.registerConfig( c2 );
        cm.registerConfig( c4 );
        cm.registerConfig( c3 );
        cm.registerConfig( c5 );

        //inserting group before page is existing
        cm.addUiGroup( g2 );
        cm.addUiGroup( g1 );
        cm.addUiPage( p );
        cm.addUiPage( p2 );

        c1.setValue( "config1" );
    }

}

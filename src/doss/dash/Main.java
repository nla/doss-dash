package doss.dash;

import doss.dash.*;
import static spark.Spark.*;
import spark.template.velocity.*;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import java.util.*;
import java.text.*;

import doss.dash.Database;

public class Main {
	public static void main(String[] args) {
        System.out.println("DOSS_HOME is " + System.getProperty("doss.home"));
        staticFileLocation("/static");

        get("/index.html", (req, res) -> { return new Dash().index();}, new VelocityTemplateEngine());
        get("/getdata", (req, res) -> { res.type("application/json"); return new Dash().json(req.queryParams("graph"));});
    }
}

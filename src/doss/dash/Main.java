package doss.dash;

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
import doss.dash.Dash;

public class Main {
	public static void main(String[] args) {
        System.out.println("DOSS_HOME is " + System.getProperty("doss.home"));

        staticFileLocation("/static");

        int PORT = Integer.parseInt(System.getProperty("port"));
        if (PORT > 1024) {
            port(PORT);
        }

        get("/index.html", (req, res) -> { return new Dash().index();}, new VelocityTemplateEngine());
        get("/getdata", (req, res) -> { res.type("application/json"); return new Dash().json(req.queryParams("graph"));});
        get("/raw", (req, res) -> { return new Dash().rawdata();}, new VelocityTemplateEngine());
        get("/report", (req, res) -> { return new Dash().report();}, new VelocityTemplateEngine());
        get("/reportdata", (req, res) -> { res.type("application/json"); return new Dash().getReportData(req.queryParams("fy"),req.queryParams("graph"));});
    }
}

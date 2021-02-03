var express = require("express");
var app = express();
var path = require('path');
var AWS = require('aws-sdk');
var multer = require('multer');
var bodyParser = require('body-parser');
const fs = require("fs");
const csvSync = require('csv-parse/lib/sync');
var serveStatic = require('serve-static');

var vcap_services = JSON.parse(process.env.VCAP_SERVICES);

// HANA Client
var hana = require('@sap/hana-client');
const { SSL_OP_SSLEAY_080_CLIENT_DH_BUG } = require('constants');
var conn = hana.createConnection();

// HANA Connection Settings
var conn_params = {
    serverNode: vcap_services.hana[0].credentials.host + ":" + vcap_services.hana[0].credentials.port,
    encrypt: true,
    schema: vcap_services.hana[0].credentials.schema,
    sslValidateCertificate: false,
    uid: vcap_services.hana[0].credentials.user,
    pwd: vcap_services.hana[0].credentials.password
};

// Multer storage options
var storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, '/app/public/temp/');
        // Note: This is not stored permanently due to the Cloud Foundry specification.
    },
    filename: function (req, file, cb) {
        //Add timestamp to file name (for avoid conflict)
        cb(null, Date.now() + '-' + file.originalname);
    }
});

var upload = multer({ storage: storage });


// Insert loop - use autocommit
app.post("/autocommitinsert", upload.single('csvfile'), function (req, res) {

    let csvdata = fs.readFileSync('/app/public/temp/' + req.file.filename);    
    let csvarray = csvSync(csvdata);

    try {
        conn.connect(conn_params);
    } catch (err) {
        if (err.code != -20004) {
            console.log("DB Error: DB Connection --- ", err);
            res.send("Connection Error");
            return;
        }
    }
    if (conn.state == "disconnected") {
        console.log("DB Error: DB Connection  --- ", err);
        res.send("Connection Error");
        return;
    }

    var sql = 'delete from "HCIMPORTSAMPLE_HDI_DB_1"."hcimportsample.db::testtable" ;';
    var stmt = conn.prepare(sql);
    try {
        stmt.exec();
    } catch (err) {
        console.log("DELETE ERROR --- ");
        console.log("SQL=", sql);
        console.log("DB Error: SQL Execution --- ", err);
        var resultjson = {};
        resultjson.message = "NG";
        resultjson.reason = "DELETE ERROR.";
        res.json(resultjson);
        return;
    }

    var sql = 'INSERT INTO "HCIMPORTSAMPLE_HDI_DB_1"."hcimportsample.db::testtable" VALUES(?,?,?,?,?,?,?,?,?,?);';
    var stmt = conn.prepare(sql);
    for (let i=0;i<csvarray.length;i++){
        try {
            stmt.exec([csvarray[i][0],csvarray[i][1],csvarray[i][2],csvarray[i][3],csvarray[i][4],csvarray[i][5],csvarray[i][6],csvarray[i][7],csvarray[i][8],csvarray[i][9]]);
        } catch (err) {
            console.log("Insert error --- ", i.toString());
            console.log("SQL=", sql);
            console.log("DB Error: SQL Execution --- ", err);
            var resultjson = {};
            resultjson.message = "NG";
            resultjson.reason = "INSERT  ERROR.";
            res.json(resultjson);
            return;
        }
    }
    var resultjson = {};
    resultjson.message = "OK";
    resultjson.reason = "AUTOCOMMIT INSERT FINISHED.";
    res.json(resultjson);
    return;
});

// Insert loop - transaction
app.post("/noautocommitinsert", upload.single('csvfile'), function (req, res) {

    let csvdata = fs.readFileSync('/app/public/temp/' + req.file.filename);    
    let csvarray = csvSync(csvdata);

    try {
        conn.connect(conn_params);
    } catch (err) {
        if (err.code != -20004) {
            console.log("DB Error: DB Connection --- ", err);
            res.send("Connection Error");
            return;
        }
    }
    if (conn.state == "disconnected") {
        console.log("DB Error: DB Connection  --- ", err);
        res.send("Connection Error");
        return;
    }

    var sql = 'delete from "HCIMPORTSAMPLE_HDI_DB_1"."hcimportsample.db::testtable" ;';
    var stmt = conn.prepare(sql);
    try {
        stmt.exec();
    } catch (err) {
        console.log("DELETE ERROR --- ");
        console.log("SQL=", sql);
        console.log("DB Error: SQL Execution --- ", err);
        var resultjson = {};
        resultjson.message = "NG";
        resultjson.reason = "DELETE ERROR.";
        res.json(resultjson);
        return;
    }

    //Disable Autocommit
    conn.setAutoCommit(false);

    var sql = 'INSERT INTO "HCIMPORTSAMPLE_HDI_DB_1"."hcimportsample.db::testtable" VALUES(?,?,?,?,?,?,?,?,?,?);';
    var stmt = conn.prepare(sql);
    for (let i=0;i<csvarray.length;i++){
        try {
            stmt.exec([csvarray[i][0],csvarray[i][1],csvarray[i][2],csvarray[i][3],csvarray[i][4],csvarray[i][5],csvarray[i][6],csvarray[i][7],csvarray[i][8],csvarray[i][9]]);
        } catch (err) {
            console.log("Insert error --- ", i.toString());
            console.log("SQL=", sql);
            console.log("DB Error: SQL Execution --- ", err);
            var resultjson = {};
            resultjson.message = "NG";
            resultjson.reason = "INSERT  ERROR.";
            res.json(resultjson);
            return;
        }
    }

    conn.commit();

    conn.setAutoCommit(true);

    var resultjson = {};
    resultjson.message = "OK";
    resultjson.reason = "1 TRANSACTION INSERT FINISHED.";
    res.json(resultjson);
    return;
    
});

// Upload to Object store , then import
app.post("/useobjstore", upload.single('csvfile'), function (req, res) {
    let csvdata = fs.readFileSync('/app/public/temp/' + req.file.filename);    

    //Object Storeにアップロード
    const credentials = new AWS.Credentials({ accessKeyId: vcap_services.objectstore[0].credentials.access_key_id, secretAccessKey: vcap_services.objectstore[0].credentials.secret_access_key });
    AWS.config.credentials = credentials;
    AWS.config.update({ region: vcap_services.objectstore[0].credentials.region });


    var s3 = new AWS.S3();
    var params = {
        Bucket: vcap_services.objectstore[0].credentials.bucket,
        Key: req.file.filename
    };

    console.log("S3 Upload file : " + req.file.filename);

    params.Body = csvdata;


    try {
        conn.connect(conn_params);
    } catch (err) {
        if (err.code != -20004) {
            console.log("DB Error: DB Connection --- ", err);
            res.send("Connection Error");
            return;
        }
    }
    if (conn.state == "disconnected") {
        console.log("DB Error: DB Connection  --- ", err);
        res.send("Connection Error");
        return;
    }

    var sql = 'delete from "HCIMPORTSAMPLE_HDI_DB_1"."hcimportsample.db::testtable" ;';
    var stmt = conn.prepare(sql);
    try {
        stmt.exec();
    } catch (err) {
        console.log("DELETE ERROR --- ");
        console.log("SQL=", sql);
        console.log("DB Error: SQL Execution --- ", err);
        var resultjson = {};
        resultjson.message = "NG";
        resultjson.reason = "DELETE ERROR.";
        res.json(resultjson);
        return;
    }

    var putObjectPromise = s3.putObject(params).promise();
    putObjectPromise.then(function (data) {

        var sql = "IMPORT FROM CSV FILE 's3-" + vcap_services.objectstore[0].credentials.region + "://" +
            vcap_services.objectstore[0].credentials.access_key_id + ":" + vcap_services.objectstore[0].credentials.secret_access_key + "@" +
            vcap_services.objectstore[0].credentials.bucket + "/" + req.file.filename + "' INTO " +
            '"HCIMPORTSAMPLE_HDI_DB_1"."hcimportsample.db::testtable";';

        var stmt = conn.prepare(sql);

        try {
            stmt.exec();
        } catch (err) {
            console.log("IMPORT ERROR --- ");
            console.log("SQL=", sql);
            console.log("DB Error: SQL Execution --- ", err);

            var resultjson = {};
            resultjson.message = "NG";
            resultjson.reason = "FILE Import error .";
            res.json(resultjson);
            return;

        }


        var resultjson = {};
        resultjson.message = "OK";
        resultjson.reason = "File Imported";
        res.json(resultjson);
        return;

    }).catch(function (err) {
        console.log(err);
        var resultjson = {};
        resultjson.message = "NG";
        resultjson.reason = "Can't upload to Object Store .";
        res.json(resultjson);
        return;
    });


    
});


app.use(serveStatic(__dirname + "/public"));

app.listen(process.env.PORT || 9000);

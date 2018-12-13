var mysql = require('mysql');
var LineByLineReader = require('line-by-line');

var RC_2007 = 'jsonDataBase/RC_2007-10';
var RC_2007_small = 'jsonDataBase/RC_2007-10-small';
var RC_2011 = 'jsonDataBase/RC_2011-07';
var RC_2012 = 'jsonDataBase/RC_2012-12';
var test = 'jsonDataBase/testQ';

var con;
var lines = [];
var index = 0;
var index2 = 1;
var totalMillieSeconds = 0;

connect(RC_2012);

/**
 * Connects to databse.
 * Todo: Write timestamps to logfile.
 */
function connect(fileAdr) {

    con = mysql.createConnection({
        host: "localhost",
        user: "root",
        password: "",
        database: "reddit"
      });
      
    con.connect(function(err) {
        if (err) throw err;
        console.log("Connected!");
        try {
            //createDatabase();

            dropCommentsTable()
            .then((result) => {
                console.log(result);
                createCommentsTable(true)
                .then((result) => {
                    console.log(result);
                    query("set names 'utf8mb4'");
                    readJSON(fileAdr);
                })
            })
            .catch((err) => {
                console.log(err);
            });

            //var queryAll = "SELECT * FROM comments";
            //query(queryAll);
        } catch (error) {
            console.error(error);
        }
    });
}

/**
 * Reads from json file, line by line
 * @param {String} fileAdr 
 */
function readJSON(fileAdr) {
    var lr = new LineByLineReader(fileAdr);
    
    lr.on('error', function (err) {
        console.error(err);
    });
    
    var maxLength = 0;
    //'line' contains the current line without the trailing newline character.
    lr.on('line', function (line) {
        var comment = JSON.parse(line);
        index++;
        
        lines.push([
            comment.id,
            comment.parent_id,
            comment.link_id,
            comment.name,
            comment.author,
            comment.body,
            comment.subreddit_id,
            comment.subreddit,
            comment.score,
            comment.created_utc]
        );
        /*
        if (comment.body.length > maxLength) {
            maxLength = comment.body.length;
            console.log("Max length body:  " + maxLength);
        }*/

        if(index === 50000) {
            console.log(index*index2 + " total records.");
            index2++;
            //console.log("COMMENT ID: "+comment.id);
            lr.pause();
            insertIntoComments(lines)
            .then( ()=> {
                index = 0;
                lines = [];
                lr.resume();
            });

        }
       
    });

    /**
     * All lines are read, file is closed now.
     * Write timestamps to logfile.
     */
    lr.on('end', function () {
        console.log(index*index2 + " total records.");
        if(index > 0) {
            insertIntoComments(lines);
        }
        console.log("\n\n");
        console.log("All lines are read, file is closed now.");
    });
}

/**
 * Insert into comments table.
 * @param {Array} values 
 */
function insertIntoComments(values) {

    var start = Date.now();
    var sql = "INSERT INTO comments (id, parent_id, link_id, name, author, body, subreddit_id, subreddit, score, created_utc) VALUES ?";
    return new Promise((resolve) => {
        con.query(sql, [values], function (err, result) {
            if (err) throw err;
            var end = Date.now() - start;
            console.log("Number of records inserted: " + result.affectedRows);
            totalMillieSeconds += end;
            console.log("AFTER:" + end + " ms. " + "TOTAL:" + Math.floor(totalMillieSeconds/1000) + " seconds.\n");
            resolve();
          });
    });
}

/**
 * Query the database connected to.
 * @param {String} sql 
 */
function query(sql) {
    return new Promise(() => {
        con.query(sql, function (err, result, fields) {
            if (err) throw err;
            console.log(result);
        });
    });
}

/**
 * Create a new Database.
 * @param {function} callback 
 */
function createDatabase(callback) {
    return con.query("CREATE DATABASE reddit", function (err, result) {
        if (err) throw err;
        console.log("Database created");
        callback(result);
    });
}

/**
 * Create comments table in connected database.
 * @param {boolean} constraints use constarints for the table
 */
function createCommentsTable(constraints) {
    //todo: add constraints
    if(constraints) {
        var sql = "CREATE TABLE comments (id VARCHAR(10) PRIMARY KEY, parent_id VARCHAR(10) NOT NULL, link_id VARCHAR(10) NOT NULL, name VARCHAR(20) NOT NULL, author VARCHAR(20) NOT NULL, body MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL, subreddit_id VARCHAR(10) NOT NULL, subreddit VARCHAR(20) NOT NULL, score INT, created_utc VARCHAR(10) NOT NULL)";
    } else {
        var sql = "CREATE TABLE comments (id VARCHAR(10), parent_id VARCHAR(10), link_id VARCHAR(10), name VARCHAR(20), author VARCHAR(20), body MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci, subreddit_id VARCHAR(10), subreddit VARCHAR(20), score INT, created_utc VARCHAR(10))";
    }

    return new Promise((resolve) => {
        con.query(sql, function (err, result) {
            if (err) throw err;
            console.log("Table created");
            resolve(result);
        });
    });
}

/**
 * Drops comments table from database.
 */
function dropCommentsTable () {
    var sql = "DROP TABLE comments";
    return new Promise((resolve) => {
        con.query(sql, function (err, result) {
            if (err) throw err;
            console.log("Table deleted");
            resolve(result);
        });
    });
}

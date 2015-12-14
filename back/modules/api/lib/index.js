/**
 * API
 */
var mysql = require('mysql');
var ejs = require('ejs');
var archiver = require('archiver');
var fs = require('fs');
var memStream = require('memory-streams');
var AWS = require('aws-sdk');
var helpers = require('./queryHelpers.js')

var slideBucket = process.env.SLIDE_BUCKET.replace('#region#', (AWS.config.region || 'eu-west-1').replace(/-/g, '')); 
var s3 = new AWS.S3();
var lambda = new AWS.Lambda();

function getByKeyword(connection, queryParams, success, error) {

  if (queryParams.q === undefined) {
    throw "Missing keyword parameter. Use q=YOUR-KEYWORD"
  }
  console.log(AWS.config.region);
  var params = { startdate: undefined, enddate: undefined, category: undefined };
  helpers.sanitizeParams(params, queryParams);
  var queryString = helpers.buildQueryString(params.startdate, params.enddate, params.category, queryParams.q);

  connection.query(queryString, function (err, result) {
    if (err) {
      error(err)
    }
    else if (result) {
      success(result);
    }
  });
}

function getFeatures(connection, queryParams, success, error) {

  var params = { startdate: undefined, enddate: undefined, category: undefined };
  helpers.sanitizeParams(params, queryParams);
  var queryString = helpers.buildQueryString(params.startdate, params.enddate, params.category);

  connection.query(queryString, function (err, result) {
    if (err) {
      error(err);
    }
    else if (result) {
      success(result);
    }
  });
}

function generateSlideLink(params) {
  var cat = (params.category === undefined) ? '' : '-' + params.category
  var slidedeckName = 'slideheap/' + params.startdate + '-' + params.enddate + cat + '.zip';
  
  return slidedeckName;
}

function checkSlidedeck(name, cb)
{
  s3.getObject({ Bucket: 'serverless.euwest1.awsfeatureroll.com', Key: name }, function(err, data) { 
    cb((err) ? false : true);
  });
}

function getSlideUrl(queryParams, success, error) {
  var params = { startdate: undefined, enddate: undefined, category: undefined };
  helpers.sanitizeParams(params, queryParams);
  
  var s3Key = generateSlideLink(params);
  var signedUrl = s3.getSignedUrl('getObject', { Bucket: slideBucket, Key: s3Key, Expires: 3600 });
  
  checkSlidedeck(s3Key, function(exists) {
    if(exists) {    
      return success( {
        cached: true,
        link: signedUrl   
      });
    }
    else {
      var payload = {
        startdate: params.startdate,
        enddate: params.enddate,
        category: params.category,
        slideBucket: slideBucket,
        s3Key: s3Key 
      };
      
      var lambdaParams = {
        FunctionName: process.env.API_CREATESLIDEDECK_ARN,
        InvocationType: 'Event',
        Payload: JSON.stringify(payload),
        Qualifier: process.env.SERVERLESS_STAGE
      };
      
      // If successfull returns 202 statuscode
      lambda.invoke(lambdaParams).send(function(err, data){
        if (err) 
          error(err);
        
        return success({
          cached: false,
          link: signedUrl
        });
      });
    }
  });
}

function createSlidedeck(connection, event, success, error) {
  var params = { startdate: event.startdate, enddate: event.enddate, category: event.category };
  var queryString = helpers.buildQueryString(params.startdate, params.enddate, params.category);
  params.slideBucket = event.slideBucket;
  params.s3Key = event.s3Key;

  connection.query(queryString, function (err, result) {
    if (err) {
      error(err);
    }
    else if (result) {
      archiveSlidedeck(result, params, success, error);
    }
  });
}

function archiveSlidedeck(data, params, success, error) {
  data = JSON.stringify(data);
  var result = JSON.parse(data);
  var features = {};
  for (var i = 0; i < result.length; ++i) {

    if (features[result[i].category] === undefined) {
      features[result[i].category] = [];
    }

    features[result[i].category].push(result[i]);
  }

  var author = params.author || 'Amazon Web Services';
  var twitter = params.twitter || 'AWS_Aktuell';
  var title = params.title || 'AWS Feature Update';

  var templateDir = __dirname + "/../slides/revealjs-template/";

  fs.readFile(templateDir + "revealjs-slides.ejs", "utf-8", function (err, data) {

    if (err) {
      throw err;
    }

    var html = ejs.render(data, {
      allFeatures: features,
      author: author,
      title: title,
      twitter: twitter,
      startdate: params.startdate,//.format('MMMM Do'),
      enddate: params.enddate//.format('MMMM Do YYYY')
    });

    var htmlStream = new memStream.ReadableStream;
    htmlStream.push(html);
    var archive = archiver('zip');
    archive.on('error', function (err) {
      error(err);
    });

    archive.bulk([
      { expand: true, cwd: templateDir + 'res', src: ['**'] }
    ]);
    archive.append(htmlStream, { name: 'index.html' });
    archive.finalize();
    var s3Slides = new AWS.S3({ params: {region:'eu-west-1', Bucket:params.slideBucket, Key: params.s3Key}});    
    s3Slides.upload({Body: archive}).
    send(function(err, data) {
      if (err) return error(err);
      else return success(data);  
    });
  });
}

// TODO: Integrate pooling
var connection = mysql.createConnection({
  host: process.env.DBHOST,
  user: process.env.DBUSER,
  password: process.env.DBPASSWORD,
  database: process.env.DATABASE,
  port: 3306
});

connection.connect();

module.exports = {
  getFeatures: function (event, cb) {
    getFeatures(connection, event,
      function (result) {
        return cb(null, result);
      },
      function (err) {
        return cb(err);
      });
  },

  getByKeyword: function (event, cb) {
    getByKeyword(connection, event,
      function (result) {
        return cb(null, result);
      },
      function (err) {
        return cb(err);
      });
  },

  getSlideUrl: function (event, cb) {
    getSlideUrl(event,
      function (result) {
        return cb(null, result);
      },
      function (err) {
        return cb(err);
      });
  },

  createSlidedeck: function (event, cb) {
    createSlidedeck(connection, event,
      function (result) {
        return cb(null, result);
      },
      function (err) {
        return cb(err);
      });
  }
}
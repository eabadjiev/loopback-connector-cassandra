/**
* Created by Amit Ashtekar
  Date :4/22/2015
*/

var cassandra_driver = require('cassandra-driver');
var cassandraDB = require('./cassandra_db');
var debug = require('debug')('loopback:connector:cassandra');

/**
 * Initialize the cassandra connector for the given data source
 * @param {DataSource} dataSource The data source instance
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {

  if (!cassandra_driver) {
    return;
  }

  var dbSettings = dataSource.settings || {};
  var connector = new cassandraDB(cassandra_driver, dbSettings);
  dataSource.connector = connector;
  dataSource.connector.dataSource = dataSource;
  dataSource.connector.dataSource.log = function (msg) {
   console.log(msg);
  };
 console.log(JSON.stringify(callback));
  if(callback){
    dataSource.connector.connect(callback);
    process.nextTick(callback);
  }
};

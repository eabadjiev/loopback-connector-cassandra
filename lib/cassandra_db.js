/**
 * Created by Amit Ashtekar
 Date :4/22/2015
 */

var cassandraConnector = require('loopback-connector').SqlConnector,
    util  = require('util'),
    async = require('async'),
    debug = require('debug')('loopback:connector:cassandra');

var NAME = 'cassandra_driver';

var client = null,
    contact_Points = [],
    clustaring_Keys=[];


/**
 * Constructor for cassandra connector
 * @param {Object} settings The settings object
 * @param {DataSource} dataSource The data source
 * instance
 * @constructor
 */
var cassandraDB = function(cassandra_driver, dbSettings) {

  if (!(this instanceof cassandraDB)) {
    return new cassandraDB(cassandra_driver, dbSettings);
  }

  cassandraConnector.call(this, NAME, dbSettings);

  this.name = NAME;
  this.settings = dbSettings;
  this.cassandra_driver = cassandra_driver;
  this.current_order = [];
  this.contactPoints  = dbSettings.contactPoints ;

};

module.exports = cassandraDB;
util.inherits(cassandraDB, cassandraConnector);

/**
 * Get the default data type for ID
 * @returns {Function} The default type for ID
 */
cassandraDB.prototype.getDefaultIdType = function () {
  return Number;
};

/**
 * Connect to the Database
 * @param callback
 */
cassandraDB.prototype.connect = function (callback) {
  var self = this;

  contact_Points = ((self.settings.contactPoints  != null) ? (self.settings.contactPoints) : ["127.0.0.1"]);
  key_space=((self.settings.keyspace) ? (self.settings.keyspace) : undefined);
  clustaring_Keys=((self.settings.clustaringKeys) ? (self.settings.clustaringKeys) : undefined);
  partition_Key=((self.settings.partitionKey) ? (self.settings.partitionKey) : undefined);
  if(!key_space)
  {
    return console.log("keyspace is not defined!");
  }
  clustaring_Keys.forEach(function (val){
    if(partition_Key===val)
    {
      return callback("partition key can not be a clustating key !!", null);

    }
  });


  client =new self.cassandra_driver.Client( { contactPoints : contact_Points ,keyspace:key_space,clustaringKeys:clustaring_Keys,partitionKey:partition_Key} );
  client.connect(function(error,result){

    if(error){
      self.log("Not Connected", error);
      callback(error, null);
    } else {
      client.options.queryOptions.prepare=true;
      self.log("Connected to Database!");
      callback(null, 'Connected');
    }
  });

};
cassandraDB.prototype.create = function create(model, data, callback) {


  var self = this;
  data = self.mapToDB(model, data);
  var props = self._categorizeProperties(model, data);
  var sql = [];
  sql.push('INSERT INTO ', self.tableEscaped(model), ' ',
      self.toFields(model, data, true));
  var idColName = self.idColumn(model);
  this.query(sql.join(''), generateQueryParams(data, props), function (err, lastUpdatedID) {
    if (err) {
      return callback(err);
    }
    callback(err, lastUpdatedID);
  });
};
cassandraDB.prototype.mapToDB = function (model, data) {
  var dbData = {};
  if (!data) {
    return dbData;
  }
  var props = this._models[model].properties;
  for (var p in data) {
    if(props[p]) {
      var pType = props[p].type && props[p].type.name;
      if (pType === 'GeoPoint' && data[p]) {
        dbData[p] = '(' + data[p].lat + ',' + data[p].lng + ')';
      } else {
        dbData[p] = data[p];
      }
    }
  }
  return dbData;
};

cassandraDB.prototype._categorizeProperties = function(model, data) {
  var ids = this.idNames(model);
  var idsInData = ids.filter(function(key) {
    return data[key] !== null && data[key] !== undefined;
  });
  var props = Object.keys(this._models[model].properties);
  var nonIdsInData = Object.keys(data).filter(function(key) {
    return props.indexOf(key) !== -1 && ids.indexOf(key) === -1 && data[key] !== undefined;
  });

  return {
    ids: ids,
    idsInData: idsInData,
    nonIdsInData: nonIdsInData
  };
};

cassandraDB.prototype.tableEscaped = function (model) {
  return this.escapeName(this.table(model));
};

cassandraDB.prototype.escapeName = function (name) {
  if (!name) {
    return name;
  }
  return '"' + name.replace(/\./g, '"."') + '"';
};

cassandraDB.prototype.toFields = function (model, data, forCreate) {
  var self = this;
  var props = self._categorizeProperties(model, data);
  var dataIdNames = props.idsInData;
  var nonIdsInData = props.nonIdsInData;
  var query = [];
  if (forCreate) {
    if(nonIdsInData.length == 0 && dataIdNames.length == 0) {
      return 'default values ';
    }
    query.push('(');
    query.push(props.ids.map(function (key) {
      return self.columnEscaped(model, key)+",";
    }).join(','));
    query.push(nonIdsInData.map(function (key) {
      return self.columnEscaped(model, key);
    }).join(','));
    if (dataIdNames.length > 0) {
      if (nonIdsInData.length > 0) {
        query.push(',');
      }
      query.push(dataIdNames.map(function (key) {
        return self.columnEscaped(model, key);
      }).join(','));
    }
    query.push(') VALUES (');
    //my comment
    //var i=1
    for (var i = 0, len = nonIdsInData.length + dataIdNames.length; i <= len; i++) {
      //my Comment
      query.push('?');
      if (i !== len) {
        query.push(',');
      }
    }
    query.push(') ');
  } else {
    query.push(nonIdsInData.map(function (key, i) {
      //my comment
      // return self.columnEscaped(model, key) + "=$" + (i + 1)
      return self.columnEscaped(model, key) + "=?"
    }).join(','));
  }
  return query.join('');
};
function generateQueryParams(data, props,forupdate) {
  var uniqueno=0;
  var queryParams = [];

  function pushToQueryParams(key) {
    queryParams.push(data[key] !== undefined ? data[key] : null);
  }
  function pushToQueryParams_forAutoId() {
    queryParams.push(uniqueNo());
  }
  function uniqueNo()
  {
    var now=new Date();

    return uniqueno=parseInt(now.getMilliseconds().toString()+now.getHours().toString()+now.getMinutes().toString()+now.getSeconds().toString()+now.getMilliseconds().toString());
    return uniqueno=parseInt(now.getDate().toString()+now.getHours().toString()+now.getMinutes().toString()+now.getSeconds().toString());
    //   var time = new Date().getTime();
    //  while (time == new Date().getTime());
    //  return parseInt(new Date().getTime());
  }
  props.ids.forEach(pushToQueryParams_forAutoId);
  props.nonIdsInData.forEach(pushToQueryParams);
  props.idsInData.forEach(pushToQueryParams);
//my comment
  if(forupdate){
    var a=[];
    queryParams.forEach(function (val){
      if(val===data.Language)
      {
        a.push(val);
      }
    });
    return a;
  }
  else {
    return queryParams;
  }
};

cassandraDB.prototype.query = function (sql, params, callback) {

  if (!callback && typeof params === 'function') {
    callback = params;
    params = [];
  }

  params = params || [];

  for(var i=0; i < params.length; i++){
    if(typeof params[i] == 'object') {
      // Exclude Date objects from getting converted
      if(isNaN(new Date(params[i]).getTime())) {
        params[i] = JSON.stringify(params[i]);
      }
    }
  }

  var cb = callback || function (err, result) {
      };
  this.executeSQL(sql, params, cb);
};


cassandraDB.prototype.executeSQL = function (sql, params, callback) {

  var self = this;
  var time = Date.now();
  var log = self.log;

  if (self.settings.debug) {
    if (params && params.length > 0) {
      debug('SQL: ' + sql + '\nParameters: ' + params);
    } else {
      debug('SQL: ' + sql);
    }
  }

  if(params.status && sql.startsWith('PRAGMA')) {

    var stmt = client._getprepare(sql);
    client.all(sql, function(err, rows) {
      // if(err) console.error(err);
      if (err && self.settings.debug) {
        debug(err);
      }
      if (self.settings.debug && rows) debug("%j", rows);
      if (self.debug) {
        log(sql, time);
        // log(rows, time);
      }

      var new_rows = [];
      for(var i=0; i < rows.length; i++) {
        var temp = {};
        temp['column'] = rows[i].name;
        temp['type'] = rows[i].type;
        temp['nullable'] = (rows[i].notnull == 0) ? 'YES' : 'NO';
        new_rows.push(temp);
      }

      callback(err ? err : null, new_rows);
    });
  } else if(sql.startsWith('INSERT') || sql.startsWith('UPDATE') ||
      sql.startsWith('DELETE') || sql.startsWith('CREATE') || sql.startsWith('DROP')) {
    if( sql.startsWith('DROP')) {
      var sqlarr = sql.split('"');
      // sqlarr[1] = String(this.settings.keyspace) + "." + sqlarr[1];
      sqlarr[1] = "\"" + sqlarr[1]+"\"";
      sql=  sqlarr.join('')+";";
    }
    client.execute(sql, params, function(err) {
      // if(err) console.error(err);
      var data = this;
      if (err && self.settings.debug) {
        debug(err);
      }
      if (self.settings.debug && data) debug("%j", data);
      if (self.debug) {
        log(sql, time);
        // log("Last inserted id: " + data.lastID, time);
      }

      var result = null;

      if(sql.startsWith('UPDATE') || sql.startsWith('DELETE')) {
        result = {count: data.changes};
      } else {
        result = data.lastID;
      }
      if(err) {
        log(sql, err);
      }

      callback(err ? err : null, result);
    });
  } else {
    //my comment
    // client.all(sql, params, function(err, rows) {
    client.execute(sql, params, function(err, rows) {
      // if(err) console.error(err);
      if (err && self.settings.debug) {
        debug(err);
      }
      if (self.settings.debug && rows) debug("%j", rows);
      if (self.debug) {
        log(sql, time);
        // log(rows, time);
      }

      //my comment
      // callback(err ? err : null, rows);
      if (rows.rows)
      {
        callback(err ? err : null, rows.rows);
      }else
      {
        callback(err ? err : null, rows);
      }
    });
  }
};

if (!String.prototype.startsWith) {
  String.prototype.startsWith = function(searchString, position) {
    position = position || 0;
    return this.lastIndexOf(searchString, position) === position;
  };
}
cassandraDB.prototype.autoupdate = function(models, cb) {
  var self = this;
  if ((!cb) && ('function' === typeof models)) {
    cb = models;
    models = undefined;
  }
  // First argument is a model name
  if ('string' === typeof models) {
    models = [models];
  }

  models = models || Object.keys(this._models);

  async.each(models, function(model, done) {
    if (!(model in self._models)) {
      return process.nextTick(function() {
        done(new Error('Model not found: ' + model));
      });
    }
    getTableStatus.call(self, model, function(err, fields) {
      if (!err && fields.length) {
        self.alterTable(model, fields, done);
      } else {
        self.createTable(model, done);
      }
    });
  }, cb);
};
function getTableStatus(model, cb) {
  function decoratedCallback(err, data) {
    if (err) {
      console.error(err);
    }
    if (!err) {
      data.forEach(function (field) {
        field.type = mapSQLiteDatatypes(field.type);
      });
    }
    cb(err, data);
  }

  var sql = null;
  sql = 'PRAGMA table_info(' + this.table(model) +')';
  var params = {}
  params.status = true;
  this.query(sql, params, decoratedCallback);
};

cassandraDB.prototype.propertiesSQL = function (model) {
  var self = this;
  var sql = [];
  var pks = this.idNames(model).map(function (i) {
    return self.columnEscaped(model, i);
  });
  Object.keys(this._models[model].properties).forEach(function (prop) {
    var colName = self.columnEscaped(model, prop);
    sql.push(colName + ' ' + self.propertySettingsSQL(model, prop));
  });
  //my comment
  //Autogenerated "Id" will be the partition key and First model key i.e. here in this example it is "title"
  //will be the clustaring key
  //pks.push("".concat("\"",sql[0].split('"')[1],"\""));
  this.settings.clustaringKeys.forEach(function (val){
    pks.push("".concat("\"",val,"\""));
  });
  var reformattedArray=[];
  reformattedArray.push("".concat("\"",this.settings.partitionKey,"\""));
  pks.forEach(function (val){
    reformattedArray.push(val);
  });
  if (pks.length > 0) {
    sql.push('PRIMARY KEY(' + reformattedArray.join(',') + ')');
  }
  return sql.join(',\n  ');

};
cassandraDB.prototype.propertySettingsSQL = function (model, propName) {
  var self = this;
  if (this.id(model, propName) && this._models[model].properties[propName].generated) {
    //my comment
    //Default Auto generated  column :Id will have "DOUBLE" data type due to miss match encoding byte of cassandra and Javascript.
    return 'DOUBLE';
  }
  var result = self.columnDataType(model, propName);
  if (!propertyCanBeNull.call(self, model, propName)) result = result + ' NOT NULL';

  result += self.columnDbDefault(model, propName);
  return result;
};
function propertyCanBeNull(model, propName) {
  var p = this._models[model].properties[propName];
  if (p.required || p.id) {
    return false;
  }
  return !(p.allowNull === false ||
  p['null'] === false || p.nullable === false);
};

cassandraDB.prototype.columnDbDefault = function(model, property) {
  var columnMetadata = this.columnMetadata(model, property);
  var colDefault = columnMetadata && columnMetadata.dbDefault;

  return colDefault ? (' DEFAULT ' + columnMetadata.dbDefault): '';
};

cassandraDB.prototype.columnDataType = function (model, property) {
  var columnMetadata = this.columnMetadata(model, property);
  var colType = columnMetadata && columnMetadata.dataType;
  if (colType) {
    colType = colType.toUpperCase();
  }
  var prop = this._models[model].properties[property];
  if (!prop) {
    return null;
  }
  var colLength = columnMetadata && columnMetadata.dataLength || prop.length;
  if (colType) {
    return colType + (colLength ? '(' + colLength + ')' : '');
  }

  switch (prop.type.name) {
    default:
    case 'String':
    case 'JSON':
      return 'TEXT';
    case 'Text':
      return 'TEXT';
    case 'Number':
      return 'INT';
    case 'Date':
      return 'timestamp';
    case 'Timestamp':
      return 'timestamp';
    case 'GeoPoint':
    case 'Point':
      return '';
    case 'Boolean':
      return 'BOOLEAN';
  }
};

cassandraDB.prototype.find = function find(model, id, callback) {
  var sql = 'SELECT * FROM ' +
      this.tableEscaped(model);

  if (id) {
    var idVal = this.toDatabase(this._models[model].properties[this.idName(model)], id);
    sql += ' WHERE ' + this.idColumnEscaped(model) + ' = ' + idVal + ' LIMIT 1';
  }
  else {
    sql += ' WHERE ' + this.idColumnEscaped(model) + ' IS NULL LIMIT 1';
  }

  this.query(sql, function (err, data) {
    if (data && data.length === 1) {
      // data[0][this.idColumn(model)] = id;
    } else {
      data = [null];
    }
    callback(err, this.fromDatabase(model, data[0]));
  }.bind(this));
};

cassandraDB.prototype.toDatabase = function (prop, val) {
  console.log("toDatabase:",prop,val);
  if (val === null || val === undefined) {

    if (prop.autoIncrement) {
      return 'DEFAULT';
    }
    else {
      return 'NULL';
    }
  }

  if (val.constructor.name === 'Object') {

    var operator = Object.keys(val)[0]
    val = val[operator];
    if (operator === 'between') {
      return this.toDatabase(prop, val[0]) + ' AND ' + this.toDatabase(prop, val[1]);
    }
    if (operator === 'inq' || operator === 'nin') {
      for (var i = 0; i < val.length; i++) {
        val[i] = escape(val[i]);
      }
      return val.join(',');
    }
    return this.toDatabase(prop, val);
  }
  if (prop.type.name === 'Number') {
    if (!val && val !== 0) {
      if (prop.autoIncrement) {
        return 'DEFAULT';
      }
      else {
        return 'NULL';
      }
    }
    return escape(val);
  }

  if (prop.type.name === 'Date' || prop.type.name === 'DATETIME') {
    if (!val) {
      if (prop.autoIncrement) {
        return 'DEFAULT';
      }
      else {
        return 'NULL';
      }
    }
    if (!val) {
      if (prop.autoIncrement) {
        return 'DEFAULT';
      }
      else {
        return 'NULL';
      }
    }

    // Convert Date to timestamp
    return val.getTime();

  }

  if (prop.type.name === 'Boolean') {
    if (val) {
      return 1;
    } else {
      return 0;
    }
  }

  if (prop.type.name === 'GeoPoint') {
    if (val) {
      return '(' + escape(val.lat) + ',' + escape(val.lng) + ')';
    } else {
      return 'NULL';
    }
  }

  return escape(val.toString());

};


/*!
 * Convert the data from database to JSON
 *
 * @param {String} model The model name
 * @param {Object} data The data from DB
 */
cassandraDB.prototype.fromDatabase = function (model, data) {
  if (!data) {
    return null;
  }
  var props = this._models[model].properties;
  var json = {};
  for (var p in props) {
    var key = this.column(model, p);
    var val = data[key];
    if (val === undefined) {
      continue;
    }
    var prop = props[p];
    var type = prop.type && prop.type.name;
    if (prop && type === 'Boolean') {
      if(typeof val === 'number') {
        json[p] = ((val == 1) ? true : false);
      } else {
        json[p] = (val === 'Y' || val === 'y' || val === 'T' || val === 't' || val === '1');
      }
    } else if (prop && type === 'GeoPoint' || type === 'Point') {
      if (typeof val === 'string') {
        // The point format is (x,y)
        var point = val.split(/[\(\)\s,]+/).filter(Boolean);
        json[p] = {
          lat: +point[0],
          lng: +point[1]
        };
      } else if (typeof val === 'object' && val !== null) {
        // converts point to {x: lat, y: lng}
        json[p] = {
          lat: val.x,
          lng: val.y
        };
      } else {
        json[p] = val;
      }
    } else if (prop && type === 'Date'){
      json[p] = new Date(val);
    } else {
      json[p] = val;
    }
  }
  if (this.debug) {
    debug('JSON data: %j', json);
  }
  return json;
};

cassandraDB.prototype.all = function (model, filter, callback) {
  var self = this;


  // 'ORDER BY' is only performed when explicitely asked.
  filter = filter || {};
  if (!filter.order) {
    var idNames = this.idNames(model);
    if (idNames && idNames.length) {
      filter.order = idNames;
    }

    if(filter.where != undefined) {
      if(filter.order == 'id' && filter.where.id != undefined) {
        if(filter.where.id.inq != undefined){
          for(var i=0; i < filter.where.id.inq.length; i++) {
            self.current_order.push(filter.where.id.inq[i])
          }
        }
      }
    }
  }

  function reset_order(data) {
    var temp_data = [];
    for(var i=0; i< self.current_order.length; i++) {
      for(var j=0; j< data.length; j++) {
        if(self.current_order[i] == data[j].id){
          temp_data.push(data[j]);
          break;
        }
      }
    }
    return temp_data;
  }

  this.query('SELECT ' + this.getColumns(model, filter.fields) + '  FROM '
  + this.toFilter(model, filter), function (err, data) {
    if (err) {
      return callback(err, []);
    }
    if (data) {
      for (var i = 0; i < data.length; i++) {
        data[i] = this.fromDatabase(model, data[i]);
      }
    }

    if (filter && filter.include) {
      this._models[model].model.include(data, filter.include, callback);
    } else {
      var data_temp = [];
      if(self.current_order != []) {
        data_temp = reset_order(data);
        self.current_order = [];
      }

      callback(null, (data_temp.length != 0) ? data_temp : data);
    }
  }.bind(this));
};

cassandraDB.prototype.getColumns = function (model, props) {
  var cols = this._models[model].properties;
  var self = this;
  var keys = Object.keys(cols);
  if (Array.isArray(props) && props.length > 0) {
    // No empty array, including all the fields
    keys = props;
  } else if ('object' === typeof props && Object.keys(props).length > 0) {
    // { field1: boolean, field2: boolean ... }
    var included = [];
    var excluded = [];
    keys.forEach(function (k) {
      if (props[k]) {
        included.push(k);
      } else if ((k in props) && !props[k]) {
        excluded.push(k);
      }
    });
    if (included.length > 0) {
      keys = included;
    } else if (excluded.length > 0) {
      excluded.forEach(function (e) {
        var index = keys.indexOf(e);
        keys.splice(index, 1);
      });
    }
  }
  var names = keys.map(function (c) {
    return self.columnEscaped(model, c);
  });
  return names.join(', ');
};

cassandraDB.prototype.toFilter = function (model, filter) {

  var self = this;
  if (filter && typeof filter.where === 'function') {
    return self.tableEscaped(model) + ' ' + filter.where();
  }

  if (!filter) {
    return self.tableEscaped(model);
  }
  var out = self.tableEscaped(model) + ' ';
  var where = self.buildWhere(model, filter.where);
  if (where) {
    out += where;
  }

  var pagination = getPagination(filter);
  //my comment
  if(where.length===0)
  {
    filter.order=undefined;
  }
  if (filter.order) {
    var order = filter.order;
    if (typeof order === 'string') {
      order = [order];
    }
    var orderBy = '';
    filter.order = [];
    for (var i = 0, n = order.length; i < n; i++) {
      var t = order[i].split(/[\s]+/);
      var field = t[0], dir = t[1];
      filter.order.push(self.columnEscaped(model, field) + (dir ? ' ' + dir : ''));
    }
    orderBy = ' ORDER BY ' + filter.order.join(',');
    if (pagination.length) {
      out = out + ' ' + orderBy + ' ' + pagination.join(' ');
    } else {
      out = out + ' ' + orderBy;
    }
  } else {
    if (pagination.length) {
      out = out + ' '
      + pagination.join(' ');
    }
  }
  return out+';';
};
function getPagination(filter) {
  var pagination = [];
  if (filter && (filter.limit || filter.offset || filter.skip)) {
    var limit = Number(filter.limit);
    if (limit) {
      pagination.push('LIMIT ' + limit);
    }
    var offset = Number(filter.offset);
    if (!offset) {
      offset = Number(filter.skip);
    }
    if (offset) {
      pagination.push('OFFSET ' + offset);
    } else {
      offset = 0;
    }
  }
  return pagination;
}
cassandraDB.prototype.buildWhere = function (model, conds) {
  var where = this._buildWhere(model, conds);
  if (where) {
    return ' WHERE ' + where;
  } else {
    return '';
  }
};

cassandraDB.prototype._buildWhere = function (model, conds) {
  if (!conds) {
    return '';
  }
  var self = this;
  var props = self._models[model].properties;
  var fields = [];
  if (typeof conds === 'string') {
    fields.push(conds);
  } else if (util.isArray(conds)) {
    var query = conds.shift().replace(/\?/g, function (s) {
      return escape(conds.shift());
    });
    fields.push(query);
  } else {
    var sqlCond = null;
    Object.keys(conds).forEach(function (key) {
      if (key === 'and' || key === 'or') {
        var clauses = conds[key];
        if (Array.isArray(clauses)) {
          clauses = clauses.map(function (c) {
            return '(' + self._buildWhere(model, c) + ')';
          });
          return fields.push(clauses.join(' ' + key.toUpperCase() + ' '));
        }
        // The value is not an array, fall back to regular fields
      }
      if (conds[key] && conds[key].constructor.name === 'RegExp') {
        var regex = conds[key];
        sqlCond = self.columnEscaped(model, key);

        if (regex.ignoreCase) {
          sqlCond += ' ~* ';
        } else {
          sqlCond += ' ~ ';
        }

        sqlCond += "'" + regex.source + "'";

        fields.push(sqlCond);

        return;
      }
      if (props[key]) {
        var filterValue = self.toDatabase(props[key], conds[key]);
        if(props[key].type.name==="Number")
        {
          filterValue=parseFloat(filterValue);
        }
        if (filterValue === 'NULL') {
          fields.push(self.columnEscaped(model, key) + ' IS ' + filterValue);
        } else if (conds[key].constructor.name === 'Object') {
          var condType = Object.keys(conds[key])[0];
          sqlCond = self.columnEscaped(model, key);
          if ((condType === 'inq' || condType === 'nin') && filterValue.length === 0) {
            fields.push(condType === 'inq' ? '1 = 2' : '1 = 1');
            return true;
          }
          switch (condType) {
            case 'gt':
              sqlCond += ' > ';
              break;
            case 'gte':
              sqlCond += ' >= ';
              break;
            case 'lt':
              sqlCond += ' < ';
              break;
            case 'lte':
              sqlCond += ' <= ';
              break;
            case 'between':
              sqlCond += ' BETWEEN ';
              break;
            case 'inq':
              sqlCond += ' IN ';
              break;
            case 'nin':
              sqlCond += ' NOT IN ';
              break;
            case 'neq':
              sqlCond += ' != ';
              break;
            case 'like':
              sqlCond += ' LIKE ';
              filterValue += "ESCAPE '\\'";
              break;
            case 'nlike':
              sqlCond += ' NOT LIKE ';
              filterValue += "ESCAPE '\\'";
              break;
            default:
              sqlCond += ' ' + condType + ' ';
              break;
          }
          sqlCond += (condType === 'inq' || condType === 'nin')
              ? '(' + filterValue + ')' : filterValue;
          fields.push(sqlCond);
        } else {
          //my comment
          // fields.push(self.columnEscaped(model, key) + ' = ' + filterValue);
          if(props[key].type.name==="Number")
          {
            fields.push(self.columnEscaped(model, key) + ' = ' + filterValue);
          }
          else {
            fields.push(self.columnEscaped(model, key) + ' = ' + "".concat("\'", filterValue, "\'"));
          }
        }
      }
    });
  }
  return fields.join(' AND ');
};

cassandraDB.prototype.autoupdate = function(models, cb) {
  var self = this;
  if ((!cb) && ('function' === typeof models)) {
    cb = models;
    models = undefined;
  }
  // First argument is a model name
  if ('string' === typeof models) {
    models = [models];
  }

  models = models || Object.keys(this._models);

  async.each(models, function(model, done) {
    if (!(model in self._models)) {
      return process.nextTick(function() {
        done(new Error('Model not found: ' + model));
      });
    }
    getTableStatus.call(self, model, function(err, fields) {
      if (!err && fields.length) {
        self.alterTable(model, fields, done);
      } else {
        self.createTable(model, done);
      }
    });
  }, cb);
};

cassandraDB.prototype.update =
    cassandraDB.prototype.updateAll = function (model, where, data, callback) {
      //my comment
      var forUpdate=true
      //
      var whereClause = this.buildWhere(model, where);

      var sql = ['UPDATE ', this.tableEscaped(model), ' SET ',
            this.toFields(model, data), ' ', whereClause].join('')+";";

      data = this.mapToDB(model, data);
      var props = this._categorizeProperties(model, data);
//my comment
      //  this.query(sql, generateQueryParams(data, props), function (err, result) {
      this.query(sql, generateQueryParams(data, props,forUpdate), function (err, result) {
        if (callback) {
          callback(err, result);
        }
      });
    };

cassandraDB.prototype.save = function (model, data, callback) {
  var self = this;
  data = self.mapToDB(model, data);
  var props = self._categorizeProperties(model, data);

  var sql = [];
  sql.push('UPDATE ', self.tableEscaped(model), ' SET ', self.toFields(model, data));
  sql.push(' WHERE ');
  props.ids.forEach(function (id, i) {
    sql.push((i > 0) ? ' AND ' : ' ', self.idColumnEscaped(model), ' = $',
        (props.nonIdsInData.length + i + 1));
  });

  self.query(sql.join(''), generateQueryParams(data, props), function (err) {
    callback(err);
  });
};

cassandraDB.prototype.destroyAll = function destroyAll(model, where, callback) {
  if (!callback && 'function' === typeof where) {
    callback = where;
    where = undefined;
  }
  this.query('DELETE FROM '
  + ' ' + this.toFilter(model, where && {where: where}), function (err, data) {
    callback && callback(err, data);
  }.bind(this));
};

cassandraDB.prototype.isActual = function(models, cb) {
  var self = this;

  if ((!cb) && ('function' === typeof models)) {
    cb = models;
    models = undefined;
  }
  // First argument is a model name
  if ('string' === typeof models) {
    models = [models];
  }

  models = models || Object.keys(this._models);

  var changes = [];
  async.each(models, function(model, done) {
    getTableStatus.call(self, model, function(err, fields) {
      changes = changes.concat(getAddModifyColumns.call(self, model, fields));
      changes = changes.concat(getDropColumns.call(self, model, fields));
      done(err);
    });
  }, function done(err) {
    if (err) {
      return cb && cb(err);
    }
    var actual = (changes.length === 0);
    cb && cb(null, actual);
  });
};

cassandraDB.prototype.debug = function () {
  if (this.settings.debug) {
    debug.apply(debug, arguments);
  }
};

cassandraDB.prototype.disconnect = function disconnect(cb) {
  client.close(cb);
};

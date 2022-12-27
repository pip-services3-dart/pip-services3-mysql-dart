import 'dart:math';

import 'package:mysql1/mysql1.dart' as mysql_driver;

import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_components/pip_services3_components.dart';
import 'package:pip_services3_mysql/src/connect/connect.dart';

/// Abstract persistence component that stores data in MySQL using plain driver.
///
/// This is the most basic persistence component that is only
/// able to store data items of any type. Specific CRUD operations
/// over the data items must be implemented in child classes by
/// accessing this.connection_ and this.client_ properties.
///
/// ### Configuration parameters ###
///
/// - [table]:                  (optional) MySQL table name
/// - [schema]:                 (optional) MySQL schema name
/// - [connection(s)]:
///   - [discovery_key]:             (optional) a key to retrieve the connection from [IDiscovery]
///   - [host]:                      host name or IP address
///   - [port]:                      port number (default: 27017)
///   - [uri]:                       resource URI or connection string with all parameters in it
/// - [credential(s)]:
///   - [store_key]:                 (optional) a key to retrieve the credentials from [ICredentialStore]
///   - [username]:                  (optional) user name
///   - [password]:                  (optional) user password
/// - [options]:
///   - [connect_timeout]:      (optional) number of milliseconds to wait before timing out when connecting a new client (default: 10000)
///
///   Note: the options below are currently not supported.
///   - [idle_timeout]:         (optional) number of milliseconds a client must sit idle in the pool and not be checked out (default: 10000)
///   - [max_pool_size]:        (optional) maximum number of clients the pool should contain (default: 10)
///
/// ### References ###
///
/// - \*:logger:\*:\*:1.0           (optional) [ILogger] components to pass log messages
/// - \*:discovery:\*:\*:1.0        (optional) [IDiscovery] services
/// - \*:credential-store:\*:\*:1.0 (optional) Credential stores to resolve credentials
///
/// ### Example ###
///
///     class MyMySqlPersistence extends MySqlPersistence<MyData> {
///       MyMySqlPersistence() : super("mydata", null);
///
///       @override
///       void defineSchema_() {
///         this.clearSchema();
///         this.ensureSchema_('CREATE TABLE `' +
///             this.tableName_! +
///             '` (id VARCHAR(32) PRIMARY KEY, `key` VARCHAR(50), `content` TEXT)');
///         this.ensureIndex_(this.tableName_! + '_key', {'key': 1}, {'unique': true});
///       }
///
///
///       Future<String?> getByName(String? correlationId, String name) async {
///         var query = "SELECT * FROM " + this.quotedTableName_() + " WHERE id=?";
///         var params = [name];
///
///         var res = await client_!.query(query, params);
///
///         if (res.toList().isEmpty)
///           this.logger_.trace(correlationId, "Nothing found from %s with name = %s",
///               [this.tableName_, name]);
///         else
///           this.logger_.trace(correlationId, "Retrieved from %s with name = %s",
///               [this.tableName_, name]);
///
///         var resValues = res.toList().isNotEmpty ? res.toList()[0].fields : null;
///         var item = this.convertToPublic_(resValues);
///
///         return item;
///       }
///
///       Future<MyData?> set(String? correlationId, MyData item) async {
///         if (item == null) {
///           return null;
///         }
///
///         // Assign unique id
///         dynamic newItem = item;
///         if (newItem.id == null && this.autoGenerateId_) {
///           newItem = (newItem as ICloneable).clone();
///           newItem.id = IdGenerator.nextLong();
///         }
///
///         var row = this.convertFromPublic_(item);
///         var columns = this.generateColumns_(row);
///         var params = this.generateParameters_(row);
///         var setParams = this.generateSetParameters_(row);
///         var values = this.generateValues_(row);
///         values.addAll(List.from(values));
///
///         var query = "INSERT INTO " +
///             this.quotedTableName_() +
///             " (" +
///             columns +
///             ") VALUES (" +
///             params +
///             ")";
///         query += " ON DUPLICATE KEY UPDATE " + setParams;
///
///         var res = await client_!.query(query, values);
///
///         query = "SELECT * FROM " + this.quotedTableName_() + " WHERE id=?";
///         res = await client_!.query(query, [item.id]);
///
///         var resValues = res.toList().isNotEmpty ? res.toList()[0].fields : null;
///         newItem = this.convertToPublic_(resValues);
///
///         logger_.trace(correlationId, "Set in %s with id = %s",
///             [this.quotedTableName_(), newItem.id]);
///
///         return newItem;
///       }
///     }
///
///     var persistence = MyMySqlPersistence();
///     persistence.configure(ConfigParams.fromTuples(["host", "localhost", "port", 27017]));
///     await persistence.open(null);
///     var item = await persistence.set(null, MyData());
///     print(item);

class MySqlPersistence<T>
    implements
        IReferenceable,
        IUnreferenceable,
        IConfigurable,
        IOpenable,
        ICleanable {
  static ConfigParams _defaultConfig = ConfigParams.fromTuples([
    "table", null,
    "schema", null,
    "dependencies.connection", "*:connection:mysql:*:1.0",

    // connections.*
    // credential.*

    "options.max_pool_size", 2,
    "options.keep_alive", 1,
    "options.connect_timeout", 5000,
    "options.auto_reconnect", true,
    "options.max_page_size", 100,
    "options.debug", true
  ]);

  ConfigParams? _config;
  IReferences? _references;
  bool _opened = false;
  bool _localConnection = true;
  List<String> _schemaStatements = [];

  // The dependency resolver.
  var dependencyResolver_ = DependencyResolver(MySqlPersistence._defaultConfig);

  // The logger.
  var logger_ = new CompositeLogger();

  // The MySQL connection component.
  MySqlConnection? connection_;

  // The MySQL connection pool object.
  mysql_driver.MySqlConnection? client_;

  // The MySQL database name.
  String? databaseName_;

  // The MySQL table object.
  String? tableName_;

  // The MySQL schema object.
  String? schemaName_;

  // Max number of objects in data pages
  int maxPageSize_ = 100;

  /// Creates a new instance of the persistence component.
  ///
  /// - [tableName]    (optional) a table name.
  /// - [schemaName]   (optional) a schema name.
  MySqlPersistence(String? tableName, String? schemaName) {
    tableName_ = tableName;
    schemaName_ = schemaName;
  }

  /// Configures component by passing configuration parameters.
  ///
  /// - [config]    configuration parameters to be set.
  @override
  void configure(ConfigParams config) {
    config = config.setDefaults(MySqlPersistence._defaultConfig);
    this._config = config;

    this.dependencyResolver_.configure(config);

    this.tableName_ = config.getAsNullableString("collection") ?? tableName_;
    this.tableName_ = config.getAsNullableString("table") ?? tableName_;
    this.schemaName_ = config.getAsNullableString("schema") ?? schemaName_;
    this.maxPageSize_ =
        config.getAsNullableInteger("options.max_page_size") ?? maxPageSize_;
  }

  /// Sets references to dependent components.
  ///
  /// - [references] 	references to locate the component dependencies.
  @override
  void setReferences(IReferences references) {
    this._references = references;
    logger_.setReferences(references);

    // Get connection
    dependencyResolver_.setReferences(references);
    connection_ = this.dependencyResolver_.getOneOptional('connection');
    // Or create a local one
    if (this.connection_ == null) {
      this.connection_ = this._createConnection();
      this._localConnection = true;
    } else {
      this._localConnection = false;
    }
  }

  /// Unsets (clears) previously set references to dependent components.
  @override
  void unsetReferences() {
    this.connection_ = null;
  }

  MySqlConnection _createConnection() {
    var connection = new MySqlConnection();

    if (this._config != null) {
      connection.configure(this._config!);
    }

    if (this._references != null) {
      connection.setReferences(this._references!);
    }

    return connection;
  }

  /// Adds index definition to create it on opening
  ///
  /// - [keys] index keys (fields)
  /// - [options] index options
  void ensureIndex_(String name, Map keys, Map? options) {
    var builder = "CREATE";
    options = options ?? {};

    if (options['unique'] != null) {
      builder += " UNIQUE";
    }

    var indexName = this.quoteIdentifier_(name);
    if (this.schemaName_ != null) {
      indexName = this.quoteIdentifier_(this.schemaName_) + "." + indexName;
    }

    builder += " INDEX " + indexName + " ON " + this.quotedTableName_();

    if (options['type'] != null) {
      builder += " " + options['type'];
    }

    var fields = "";
    for (var key in keys.keys) {
      if (fields != "") fields += ", ";
      fields += this.quoteIdentifier_(key);
      var asc = keys[key];
      if (asc < 1) fields += " DESC";
    }

    builder += "(" + fields + ")";

    this.ensureSchema_(builder);
  }

  /// Adds a statement to schema definition
  ///
  /// - [schemaStatement] a statement to be added to the schema
  void ensureSchema_(String schemaStatement) {
    this._schemaStatements.add(schemaStatement);
  }

  /// Clears all auto-created objects
  void clearSchema() {
    this._schemaStatements = [];
  }

  /// Defines database schema via auto create objects or convenience methods.
  void defineSchema_() {
    // Todo: override in chile classes
    clearSchema();
  }

  /// Converts object value from internal to public format.
  ///
  /// - [value]     an object in internal format to convert.
  /// Return converted object in public format.
  dynamic convertToPublic_(dynamic value) {
    if (value == null) return null;
    if (value is Map) {
      value = value.map((k, v) =>
          v is mysql_driver.Blob ? MapEntry(k, v.toString()) : MapEntry(k, v));
      value = Map<String, dynamic>.from(value);
    }
    var item = TypeReflector.createInstanceByType(T, null);
    try {
      item.fromJson(value);
    } on NoSuchMethodError {
      throw Exception('Data class must realize fromJson method');
    }

    return item;
  }

  /// Convert object value from public to internal format.
  ///
  /// - [value]     an object in public format to convert.
  /// Return converted object in internal format.
  dynamic convertFromPublic_(dynamic value) {
    if (value == null) return null;
    if (value is Map) return value;
    try {
      return value.toJson();
    } on NoSuchMethodError {
      throw Exception('Data class must realize toJson method');
    }
  }

  String quoteIdentifier_(String? value) {
    if (value == null || value == "") return '';

    if (value[0] == '`') return value;

    return '`' + value + '`';
  }

  String quotedTableName_() {
    if (this.tableName_ == null) {
      return '';
    }

    var builder = this.quoteIdentifier_(this.tableName_);
    if (this.schemaName_ != null) {
      builder = this.quoteIdentifier_(this.schemaName_) + "." + builder;
    }
    return builder;
  }

  /// Checks if the component is opened.
  ///
  /// Return true if the component has been opened and false otherwise.
  @override
  bool isOpen() {
    return _opened;
  }

  @override
  Future open(String? correlationId) async {
    if (this._opened) {
      return;
    }

    if (this.connection_ == null) {
      this.connection_ = this._createConnection();
      this._localConnection = true;
    }

    if (this._localConnection) {
      await this.connection_!.open(correlationId);
    }

    if (!this.connection_!.isOpen()) {
      throw new ConnectionException(
          correlationId, "CONNECT_FAILED", "MySQL connection is not opened");
    }

    this._opened = false;

    this.client_ = this.connection_!.getConnection();
    this.databaseName_ = this.connection_!.getDatabaseName();

    // Define database schema
    this.defineSchema_();

    try {
      // Recreate objects
      await this.createSchema_(correlationId);

      this._opened = true;

      this.logger_.debug(
          correlationId,
          "Connected to MySQL database %s, collection %s",
          [this.databaseName_, this.tableName_]);
    } catch (ex) {
      this.client_ == null;
      throw new ConnectionException(
              correlationId, "CONNECT_FAILED", "Connection to MySQL failed")
          .withCause(ex);
    }
  }

  /// Closes component and frees used resources.
  ///
  /// - [correlationId] 	(optional) transaction id to trace execution through call chain.
  @override
  Future close(String? correlationId) async {
    if (!this._opened) {
      return;
    }

    if (this.connection_ == null) {
      throw new InvalidStateException(
          correlationId, 'NO_CONNECTION', 'MySql connection is missing');
    }

    if (this._localConnection) {
      await this.connection_!.close(correlationId);
    }

    this._opened = false;
    this.client_ = null;
  }

  /// Clears component state.
  ///
  /// - [correlationId] 	(optional) transaction id to trace execution through call chain.
  @override
  Future clear(String? correlationId) async {
    // Return error if collection is not set
    if (this.tableName_ == null) {
      throw new Exception('Table name is not defined');
    }

    var query = "DELETE FROM " + this.quotedTableName_();

    await client_!.query(query);
  }

  Future<void> createSchema_(String? correlationId) async {
    if (this._schemaStatements == null || this._schemaStatements.length == 0) {
      return;
    }

    // Check if table exist to determine weither to auto create objects
    // Todo: include schema
    var query = "SHOW TABLES LIKE '" + this.tableName_! + "'";
    var res = await client_!.query(query);
    var exist = res.length > 0;

    // If table already exists then exit
    if (exist) return;

    logger_.debug(
        correlationId,
        'Table ' +
            this.tableName_.toString() +
            ' does not exist. Creating database objects...');

    // Run all DML commands
    for (var dml in this._schemaStatements) {
      try {
        await client_!.query(dml);
      } catch (ex) {
        logger_.error(correlationId, ex as Exception,
            'Failed to autocreate database object');
        throw ex;
      }
    }
  }

  /// Generates a list of column names to use in SQL statements like: "column1,column2,column3"
  ///
  /// - [values] an array with column values or a key-value map
  /// Return a generated list of column names
  String generateColumns_(values) {
    values = !(values is List) ? values.keys : values;

    var result = '';

    for (var value in values) {
      if (result != "") result += ",";
      result += this.quoteIdentifier_(value);
    }

    return result;
  }

  /// Generates a list of value parameters to use in SQL statements like: "$1,$2,$3"
  ///
  /// - [values] an array with values or a key-value map
  /// Return a generated list of value parameters
  String generateParameters_(values) {
    values = !(values is List) ? values.keys : values;

    var index = 1;
    var result = "";
    for (var value in values) {
      if (result != "") result += ",";
      result += "?"; // "$" + index;
      index++;
    }

    return result;
  }

  /// Generates a list of column sets to use in UPDATE statements like: column1=$1,column2=$2
  ///
  /// - [values] a key-value map with columns and values
  /// Return a generated list of column sets
  String generateSetParameters_(Map values) {
    var result = "";
    var index = 1;
    for (var column in values.keys) {
      if (result != "") result += ",";
      result += this.quoteIdentifier_(column) + "=?"; //"=$" + index;
      index++;
    }

    return result;
  }

  /// Generates a list of column parameters
  ///
  /// - [values] a key-value map with columns and values
  /// Return a generated list of column values
  List generateValues_(Map values) {
    return values.values.toList();
  }

  /// Gets a page of data items retrieved by a given filter and sorted according to sort parameters.
  ///
  /// This method shall be called by a public getPageByFilter method from child class that
  /// receives FilterParams and converts them into a filter function.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - [filter]            (optional) a filter JSON object
  /// - [paging]            (optional) paging parameters
  /// - [sort]              (optional) sorting JSON object
  /// - [select]            (optional) projection JSON object
  /// Return a requested data page.
  Future<DataPage<T>> getPageByFilter_(String? correlationId, String? filter,
      PagingParams? paging, String? sort, String? select) async {
    select = select != null ? select : "*";
    var query = "SELECT " + select + " FROM " + this.quotedTableName_();

    // Adjust max item count based on configuration
    paging = paging ?? PagingParams();
    var skip = paging.getSkip(-1);
    var take = paging.getTake(this.maxPageSize_);
    var pagingEnabled = paging.total;

    if (filter != null && filter != "") {
      query += " WHERE " + filter;
    }

    if (sort != null) {
      query += " ORDER BY " + sort;
    }

    query += " LIMIT " + take.toString();

    if (skip >= 0) {
      query += " OFFSET " + skip.toString();
    }

    var res = await client_!.query(query);

    logger_.trace(correlationId, "Retrieved %d from %s",
        [res.toList().length, this.tableName_]);

    var items =
        res.toList().map((e) => convertToPublic_(e.fields) as T).toList();

    if (pagingEnabled) {
      var query = 'SELECT COUNT(*) AS count FROM ' + this.quotedTableName_();
      if (filter != null && filter != "") {
        query += " WHERE " + filter;
      }

      var res = await client_!.query(query);
      var count = res.toList().isNotEmpty ? res.toList()[0]['count'] : 0;

      var page = new DataPage<T>(items, count);
      return page;
    } else {
      var page = new DataPage<T>(items, 0);
      return page;
    }
  }

  /// Gets a number of data items retrieved by a given filter.
  ///
  /// This method shall be called by a public getCountByFilter method from child class that
  /// receives FilterParams and converts them into a filter function.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - [filter]            (optional) a filter JSON object
  /// Return a number of objects that satifsy the filter.
  Future<int> getCountByFilter_(String? correlationId, String? filter) async {
    var query = 'SELECT COUNT(*) AS count FROM ' + this.quotedTableName_();
    if (filter != null && filter != "") {
      query += " WHERE " + filter;
    }
    var res = await client_!.query(query);

    var count = res.toList().isNotEmpty ? res.toList()[0]['count'] : 0;

    logger_.trace(
        correlationId, "Counted %d items in %s", [count, this.tableName_]);

    return count;
  }

  /// Gets a list of data items retrieved by a given filter and sorted according to sort parameters.
  ///
  /// This method shall be called by a public getListByFilter method from child class that
  /// receives FilterParams and converts them into a filter function.
  ///
  /// - [correlationId]    (optional) transaction id to trace execution through call chain.
  /// - [filter]           (optional) a filter JSON object
  /// - [paging]           (optional) paging parameters
  /// - [sort]             (optional) sorting JSON object
  /// - [select]           (optional) projection JSON object
  /// Return  a list with requested objects.
  Future<List<T>> getListByFilter_(String? correlationId, String? filter,
      String? sort, String? select) async {
    select = select != null ? select : "*";
    var query = "SELECT " + select + " FROM " + this.quotedTableName_();

    if (filter != null) {
      query += " WHERE " + filter;
    }

    if (sort != null) {
      query += " ORDER BY " + sort;
    }

    var res = await client_!.query(query);

    logger_.trace(
        correlationId, "Retrieved %d from %s", [res.length, this.tableName_]);

    var items =
        res.toList().map((e) => convertToPublic_(e.fields) as T).toList();
    return items;
  }

  Future<T?> getOneRandom_(String? correlationId, String? filter) async {
    var query = 'SELECT COUNT(*) AS count FROM ' + this.quotedTableName_();
    if (filter != null) {
      query += " WHERE " + filter;
    }

    var res = await client_!.query(query);

    var count = res.length;

    query = "SELECT * FROM " + this.quotedTableName_();

    if (filter != null) {
      query += " WHERE " + filter;
    }

    var pos = (Random().nextDouble() * count).truncate();
    query += " LIMIT 1" + " OFFSET " + pos.toString();

    res = await client_!.query(query);
    var mapItem = res.toList().isNotEmpty ? res.toList()[0].fields : null;

    if (mapItem == null)
      this.logger_.trace(
          correlationId, "Random item wasn't found from %s", [this.tableName_]);
    else
      this.logger_.trace(
          correlationId, "Retrieved random item from %s", [this.tableName_]);

    var item = this.convertToPublic_(mapItem);
    return item as T?;
  }

  /// Creates a data item.
  ///
  /// - [correlation_id]    (optional) transaction id to trace execution through call chain.
  /// - [item]              an item to be created.
  /// Return a created item.
  Future<T?> create(String? correlationId, T item) async {
    if (item == null) {
      return null;
    }

    var row = this.convertFromPublic_(item);
    var columns = this.generateColumns_(row);
    var params = this.generateParameters_(row);
    var values = this.generateValues_(row);

    var query = "INSERT INTO " +
        this.quotedTableName_() +
        " (" +
        columns +
        ") VALUES (" +
        params +
        ")";

    var res = await client_!.query(query, values);

    logger_.trace(correlationId, "Created in %s with id = %s",
        [this.quotedTableName_(), row['id']]);

    var newItem = item;
    return newItem;
  }

  /// Deletes data items that match to a given filter.
  ///
  /// This method shall be called by a public deleteByFilter method from child class that
  /// receives FilterParams and converts them into a filter function.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - [filter]            (optional) a filter JSON object.
  Future<void> deleteByFilter_(String? correlationId, String? filter) async {
    var query = "DELETE FROM " + this.quotedTableName_();
    if (filter != null) {
      query += " WHERE " + filter;
    }

    var res = await client_!.query(query);

    logger_.trace(correlationId, "Deleted %d items from %s",
        [res.toList().length, this.tableName_]);
  }
}

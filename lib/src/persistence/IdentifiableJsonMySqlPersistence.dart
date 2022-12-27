import 'dart:convert';

import 'package:pip_services3_commons/pip_services3_commons.dart';

import 'IdentifiableMySqlPersistence.dart';

/// Abstract persistence component that stores data in MySQL in JSON or JSONB fields
/// and implements a number of CRUD operations over data items with unique ids.
/// The data items must implement [IIdentifiable] interface.
///
/// The JSON table has only two fields: id and data.
///
/// In basic scenarios child classes shall only override [getPageByFilter_],
/// [getListByFilter_] or [deleteByFilter_] operations with specific filter function.
/// All other operations can be used out of the box.
///
/// In complex scenarios child classes can implement additional operations by
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
///     class MyMySqlPersistence extends IdentifiableJsonMySqlPersistence<MyData, String> {
///       MyMySqlPersistence() : super('mydata', null);
///
///       @override
///       void defineSchema_() {
///         this.clearSchema();
///         this.ensureTable_();
///         this.ensureSchema_('ALTER TABLE `' +
///             this.tableName_! +
///             '` ADD `data_key` VARCHAR(50) AS (JSON_UNQUOTE(`data`->"\$.key"))');
///         this.ensureIndex_(
///             this.tableName_! + '_json_key', {"data_key": 1}, {'unique': true});
///       }
///
///       @override
///       Future<DataPage<MyData>> getPageByFilter(
///           String? correlationId, FilterParams? filter, PagingParams? paging) async {
///         filter = filter ?? new FilterParams();
///         var key = filter.getAsNullableString('key');
///
///         var filterCondition = null;
///         if (key != null) {
///           filterCondition += "`key`='" + key + "'";
///         }
///
///         return super
///             .getPageByFilter_(correlationId, filterCondition, paging, null, null);
///       }
///     }
///
///     var persistence = MyMySqlPersistence();
///     persistence
///         .configure(ConfigParams.fromTuples(["host", "localhost", "port", 27017]));
///     await persistence.open(null);
///     var item = await persistence.create(null, MyData());
///     var page = await persistence.getPageByFilter(
///         null, FilterParams.fromTuples(["key", "ABC"]), null);
///     print(page.data);
///     var deleted = await persistence.deleteById(null, '1');
///
///
class IdentifiableJsonMySqlPersistence<T extends IIdentifiable<K>, K>
    extends IdentifiableMySqlPersistence<T, K> {
  /// Creates a new instance of the persistence component.
  ///
  /// - [tableName]    (optional) a table name.
  /// - [schemaName]    (optional) a schema name.
  IdentifiableJsonMySqlPersistence(String? tableName, String? schemaName)
      : super(tableName, schemaName);

  /// Adds DML statement to automatically create JSON(B) table
  ///
  /// - [idType] type of the id column (default: VARCHAR(32))
  /// - [dataType] type of the data column (default: JSON)
  void ensureTable_({String idType = 'VARCHAR(32)', String dataType = 'JSON'}) {
    if (this.schemaName_ != null) {
      var query = "CREATE SCHEMA IF NOT EXISTS " +
          this.quoteIdentifier_(this.schemaName_);
      this.ensureSchema_(query);
    }
    var query = "CREATE TABLE IF NOT EXISTS " +
        this.quotedTableName_() +
        " (`id` " +
        idType +
        " PRIMARY KEY, `data` " +
        dataType +
        ")";
    this.ensureSchema_(query);
  }

  /// Converts object value from internal to public format.
  ///
  /// - [value]     an object in internal format to convert.
  /// Return converted object in public format.
  @override
  dynamic convertToPublic_(value) {
    if (value == null) return null;
    return super.convertToPublic_(jsonDecode(value['data']));
  }

  /// Convert object value from public to internal format.
  ///
  /// - [value]     an object in public format to convert.
  /// Return converted object in internal format.
  @override
  dynamic convertFromPublic_(value) {
    if (value == null) return null;
    try {
      var result = {
        'id': value.id,
        'data': value is Map ? jsonEncode(value) : jsonEncode(value.toJson())
      };

      return result;
    } on NoSuchMethodError {
      throw Exception(
          'Data class must realize fromJson method or value must be Map object');
    }
  }

  /// Updates only few selected fields in a data item.
  ///
  /// - [correlation_id]    (optional) transaction id to trace execution through call chain.
  /// - [id]                an id of data item to be updated.
  /// - [data]              a map with fields to be updated.
  /// Return  the updated item.
  @override
  Future<T?> updatePartially(
      String? correlationId, K? id, AnyValueMap? data) async {
    if (data == null || id == null) {
      return null;
    }

    var query = "UPDATE " +
        this.quotedTableName_() +
        " SET `data`=JSON_MERGE_PATCH(data,?) WHERE id=?";

    var values = [jsonEncode(data.getAsObject()), id];

    var res = await client_!.query(query, values);

    query = "SELECT * FROM " + this.quotedTableName_() + " WHERE id=?";
    res = await client_!.query(query, [id]);

    logger_.trace(correlationId, "Updated partially in %s with id = %s",
        [this.tableName_, id]);

    var resValues = res.toList().isNotEmpty ? res.toList()[0].fields : null;
    var newItem = this.convertToPublic_(resValues);

    return newItem;
  }
}

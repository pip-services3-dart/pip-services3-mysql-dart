import 'package:pip_services3_commons/src/data/PagingParams.dart';
import 'package:pip_services3_commons/src/data/FilterParams.dart';
import 'package:pip_services3_commons/src/data/DataPage.dart';
import 'package:pip_services3_mysql/src/persistence/IdentifiableJsonMySqlPersistence.dart';

import '../fixtures/Dummy.dart';
import '../fixtures/IDummyPersistence.dart';

class DummyJsonMySqlPersistence
    extends IdentifiableJsonMySqlPersistence<Dummy, String>
    implements IDummyPersistence {
  DummyJsonMySqlPersistence() : super('dummies_json', null);

  @override
  void defineSchema_() {
    this.clearSchema();
    this.ensureTable_();
    this.ensureSchema_('ALTER TABLE `' +
        this.tableName_! +
        '` ADD `data_key` VARCHAR(50) AS (JSON_UNQUOTE(`data`->"\$.key"))');
    this.ensureIndex_(
        this.tableName_! + '_json_key', {"data_key": 1}, {'unique': true});
  }

  @override
  Future<int> getCountByFilter(
      String? correlationId, FilterParams? filter) async {
    filter = filter ?? new FilterParams();
    var key = filter.getAsNullableString('key');

    var filterCondition = null;
    if (key != null) {
      filterCondition += "data->key='" + key + "'";
    }

    return await super.getCountByFilter_(correlationId, filterCondition);
  }

  @override
  Future<DataPage<Dummy>> getPageByFilter(
      String? correlationId, FilterParams? filter, PagingParams? paging) async {
    filter = filter ?? new FilterParams();
    var key = filter.getAsNullableString('key');

    var filterCondition = null;
    if (key != null) {
      filterCondition += "data->key='" + key + "'";
    }

    return await super
        .getPageByFilter_(correlationId, filterCondition, paging, null, null);
  }
}

import 'package:pip_services3_commons/src/data/PagingParams.dart';
import 'package:pip_services3_commons/src/data/FilterParams.dart';
import 'package:pip_services3_commons/src/data/DataPage.dart';
import 'package:pip_services3_mysql/src/persistence/IdentifiableMySqlPersistence.dart';

import '../fixtures/Dummy2.dart';
import '../fixtures/IDummy2Persistence.dart';

class Dummy2MySqlPersistence extends IdentifiableMySqlPersistence<Dummy2, int>
    implements IDummy2Persistence {
  Dummy2MySqlPersistence() : super('dummies2', null) {
    autoGenerateId_ = false;
  }

  @override
  void defineSchema_() {
    this.clearSchema();
    this.ensureSchema_('CREATE TABLE `' +
        this.tableName_! +
        '` (id INTEGER PRIMARY KEY, `key` VARCHAR(50), `content` TEXT)');
    this.ensureIndex_(this.tableName_! + '_key', {'key': 1}, {'unique': true});
  }

  @override
  Future<DataPage<Dummy2>> getPageByFilter(
      String? correlationId, FilterParams? filter, PagingParams? paging) async {
    filter = filter ?? new FilterParams();
    var key = filter.getAsNullableString('key');

    var filterCondition = null;
    if (key != null) {
      filterCondition += "`key`='" + key + "'";
    }

    return super
        .getPageByFilter_(correlationId, filterCondition, paging, null, null);
  }

  @override
  Future<int> getCountByFilter(String? correlationId, FilterParams? filter) {
    filter = filter ?? new FilterParams();
    var key = filter.getAsNullableString('key');

    var filterCondition = null;
    if (key != null) {
      filterCondition += "`key`='" + key + "'";
    }

    return super.getCountByFilter_(correlationId, filterCondition);
  }
}

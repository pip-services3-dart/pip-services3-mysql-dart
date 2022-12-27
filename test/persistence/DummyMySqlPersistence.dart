import 'package:pip_services3_commons/src/data/PagingParams.dart';
import 'package:pip_services3_commons/src/data/FilterParams.dart';
import 'package:pip_services3_commons/src/data/DataPage.dart';
import 'package:pip_services3_mysql/src/persistence/IdentifiableMySqlPersistence.dart';

import '../fixtures/Dummy.dart';
import '../fixtures/IDummyPersistence.dart';

class DummyMySqlPersistence extends IdentifiableMySqlPersistence<Dummy, String>
    implements IDummyPersistence {
  DummyMySqlPersistence() : super('dummies', null);

  @override
  void defineSchema_() {
    this.clearSchema();
    this.ensureSchema_('CREATE TABLE `' +
        this.tableName_! +
        '` (id VARCHAR(32) PRIMARY KEY, `key` VARCHAR(50), `content` TEXT)');
    this.ensureIndex_(this.tableName_! + '_key', {'key': 1}, {'unique': true});
  }

  @override
  Future<DataPage<Dummy>> getPageByFilter(
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

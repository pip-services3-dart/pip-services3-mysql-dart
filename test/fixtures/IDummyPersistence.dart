import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_data/pip_services3_data.dart';

import 'Dummy.dart';

abstract class IDummyPersistence
    implements
        IGetter<Dummy, String>,
        IWriter<Dummy, String>,
        IPartialUpdater<Dummy, String> {
  Future<DataPage<Dummy>> getPageByFilter(
      String? correlationId, FilterParams? filter, PagingParams? paging);

  Future<int> getCountByFilter(String? correlationId, FilterParams? filter);

  Future<List<Dummy>> getListByIds(String? correlationId, List<String> ids);

  @override
  Future<Dummy?> getOneById(String? correlation_id, String id);

  @override
  Future<Dummy?> create(String? correlation_id, Dummy? item);

  @override
  Future<Dummy?> update(String? correlation_id, Dummy? item);

  Future<Dummy?> set(String? correlation_id, Dummy? item);

  @override
  Future<Dummy?> updatePartially(
      String? correlation_id, String id, AnyValueMap data);

  @override
  Future<Dummy?> deleteById(String? correlation_id, String? id);

  Future<void> deleteByIds(String? correlation_id, List<String> id);
}

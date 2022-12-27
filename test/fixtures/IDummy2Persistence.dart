import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_data/pip_services3_data.dart';

import 'Dummy2.dart';

abstract class IDummy2Persistence
    implements
        IGetter<Dummy2, int>,
        IWriter<Dummy2, int>,
        IPartialUpdater<Dummy2, int> {
  Future<DataPage<Dummy2>> getPageByFilter(
      String? correlationId, FilterParams? filter, PagingParams? paging);

  Future<int> getCountByFilter(String? correlationId, FilterParams? filter);

  Future<List<Dummy2>> getListByIds(String? correlationId, List<int> ids);

  @override
  Future<Dummy2?> getOneById(String? correlation_id, int id);

  @override
  Future<Dummy2?> create(String? correlation_id, Dummy2? item);

  @override
  Future<Dummy2?> update(String? correlation_id, Dummy2? item);

  Future<Dummy2?> set(String? correlation_id, Dummy2? item);

  @override
  Future<Dummy2?> updatePartially(
      String? correlation_id, int id, AnyValueMap data);

  @override
  Future<Dummy2?> deleteById(String? correlation_id, int? id);

  Future<void> deleteByIds(String? correlation_id, List<int> id);
}

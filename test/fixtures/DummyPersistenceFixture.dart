import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:test/expect.dart';

import 'Dummy.dart';
import 'IDummyPersistence.dart';

class DummyPersistenceFixture {
  final _dummy1 = Dummy.from(null, 'Key 1', 'Content 1');
  final _dummy2 = Dummy.from(null, 'Key 2', 'Content 2');

  IDummyPersistence _persistence;

  DummyPersistenceFixture(IDummyPersistence persistence)
      : _persistence = persistence;

  Future<void> testCrudOperations() async {
    // Create one dummy
    var dummy1 = await this._persistence.create(null, this._dummy1);

    expect(dummy1, isNotNull);
    expect(dummy1!.id, isNotNull);
    expect(_dummy1.key, dummy1.key);
    expect(_dummy1.content, dummy1.content);

    // Create another dummy
    var dummy2 = await this._persistence.create(null, this._dummy2);

    expect(dummy2, isNotNull);
    expect(dummy2!.id, isNotNull);
    expect(_dummy2.key, dummy2.key);
    expect(_dummy2.content, dummy2.content);

    var page = await this._persistence.getPageByFilter(null, null, null);
    expect(page, isNotNull);
    expect(page.data.length == 2, isTrue);

    // Update the dummy
    dummy1.content = "Updated Content 1";
    var result = await this._persistence.update(null, dummy1);

    expect(result, isNotNull);
    expect(dummy1.id, result!.id);
    expect(dummy1.key, result.key);
    expect(dummy1.content, result.content);

    // Set the dummy
    dummy1.content = "Updated Content 2";
    result = await this._persistence.set(null, dummy1);

    expect(result, isNotNull);
    expect(dummy1.id, result!.id);
    expect(dummy1.key, result.key);
    expect(dummy1.content, result.content);

    // Partially update the dummy
    result = await this._persistence.updatePartially(null, dummy1.id!,
        AnyValueMap.fromTuples(['content', 'Partially Updated Content 1']));

    expect(result, isNotNull);
    expect(dummy1.id, result!.id);
    expect(dummy1.key, result.key);
    expect('Partially Updated Content 1', result.content);

    // Get the dummy by Id
    result = await this._persistence.getOneById(null, dummy1.id!);
    // Try to get item
    expect(result, isNotNull);
    expect(dummy1.id, result!.id);
    expect(dummy1.key, result.key);
    expect('Partially Updated Content 1', result.content);

    // Delete the dummy
    result = await this._persistence.deleteById(null, dummy1.id);
    expect(result, isNotNull);
    expect(dummy1.id, result!.id);
    expect(dummy1.key, result.key);
    expect('Partially Updated Content 1', result.content);

    // Get the deleted dummy
    result = await this._persistence.getOneById(null, dummy1.id!);
    // Try to get item
    expect(result, isNull);

    var count = await this._persistence.getCountByFilter(null, null);
    expect(count, 1);
  }

  Future<void> testBatchOperations() async {
    // Create one dummy
    var dummy1 = await this._persistence.create(null, this._dummy1);
    expect(dummy1, isNotNull);
    expect(dummy1!.id, isNotNull);
    expect(this._dummy1.key, dummy1.key);
    expect(this._dummy1.content, dummy1.content);

    // Create another dummy
    var dummy2 = await this._persistence.create(null, this._dummy2);

    expect(dummy2, isNotNull);
    expect(dummy2!.id, isNotNull);
    expect(_dummy2.key, dummy2.key);
    expect(_dummy2.content, dummy2.content);

    // Read batch
    var items =
        await this._persistence.getListByIds(null, [dummy1.id!, dummy2.id!]);

    expect(items.length, 2);

    // Delete batch
    await this._persistence.deleteByIds(null, [dummy1.id!, dummy2.id!]);

    // Read empty batch
    items =
        await this._persistence.getListByIds(null, [dummy1.id!, dummy2.id!]);
    expect(items.length, 0);
  }
}

import 'dart:io';
import 'package:test/test.dart';
import 'package:pip_services3_commons/pip_services3_commons.dart';
import '../fixtures/DummyPersistenceFixture.dart';
import './DummyMySqlPersistence.dart';

void main() {
  group('DummyMySqlPersistence', () {
    late DummyMySqlPersistence persistence;
    late DummyPersistenceFixture fixture;

    var mysqlUri = Platform.environment['MYSQL_URI'];
    var mysqlHost = Platform.environment['MYSQL_HOST'] ?? 'localhost';
    var mysqlPort = Platform.environment['MYSQL_PORT'] ?? 3306;
    var mysqlDatabase = Platform.environment['MYSQL_DB'] ?? 'test';
    var mysqlUser = Platform.environment['MYSQL_USER'] ?? 'mysql';
    var mysqlPassword = Platform.environment['MYSQL_PASSWORD'] ?? 'mysql';
    if (mysqlUri == null && mysqlHost == null) {
      return;
    }

    setUp(() async {
      var dbConfig = ConfigParams.fromTuples([
        'connection.uri',
        mysqlUri,
        'connection.host',
        mysqlHost,
        'connection.port',
        mysqlPort,
        'connection.database',
        mysqlDatabase,
        'credential.username',
        mysqlUser,
        'credential.password',
        mysqlPassword
      ]);

      persistence = new DummyMySqlPersistence();
      persistence.configure(dbConfig);

      fixture = new DummyPersistenceFixture(persistence);

      await persistence.open(null);
      await persistence.clear(null);
    });

    tearDown(() async {
      await persistence.close(null);
    });

    test('Crud Operations', () async {
      await fixture.testCrudOperations();
    });

    test('Batch Operations', () async {
      await fixture.testBatchOperations();
    });
  });
}

import 'dart:io';
import 'package:pip_services3_mysql/src/connect/MySqlConnection.dart';
import 'package:test/test.dart';
import 'package:pip_services3_commons/pip_services3_commons.dart';
import '../fixtures/DummyPersistenceFixture.dart';
import './DummyMySqlPersistence.dart';

void main() {
  group('DummyMySqlConnection', () {
    late MySqlConnection connection;
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

      connection = new MySqlConnection();
      connection.configure(dbConfig);

      persistence = new DummyMySqlPersistence();
      persistence.setReferences(References.fromTuples([
        new Descriptor("pip-services", "connection", "mysql", "default", "1.0"),
        connection
      ]));

      fixture = new DummyPersistenceFixture(persistence);

      await connection.open(null);
      await persistence.open(null);
      await persistence.clear(null);
    });

    tearDown(() async {
      await persistence.close(null);
    });

    test('Connection', () async {
      expect(connection.getConnection(), isNotNull);
      expect(connection.getDatabaseName() is String, isTrue);
    });

    test('Crud Operations', () async {
      await fixture.testCrudOperations();
    });

    test('Batch Operations', () async {
      await fixture.testBatchOperations();
    });
  });
}

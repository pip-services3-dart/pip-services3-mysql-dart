import 'dart:io';
import 'package:pip_services3_mysql/src/connect/MySqlConnection.dart';
import 'package:pip_services3_mysql/src/connect/MySqlConnectionResolver.dart';
import 'package:test/test.dart';
import 'package:pip_services3_commons/pip_services3_commons.dart';

void main() {
  group('MySqlConnectionResolver', () {
    test('Connection Config', () async {
      var dbConfig = ConfigParams.fromTuples([
        'connection.host',
        'localhost',
        'connection.port',
        3306,
        'connection.database',
        'test',
        'connection.ssl',
        false,
        'credential.username',
        'mysql',
        'credential.password',
        'mysql',
      ]);
      var resolver = new MySqlConnectionResolver();
      resolver.configure(dbConfig);

      var uri = await resolver.resolve(null);
      expect(uri.isNotEmpty, isTrue);
      expect('mysql://mysql:mysql@localhost:3306/test?ssl=false', uri);
    });
  });
}

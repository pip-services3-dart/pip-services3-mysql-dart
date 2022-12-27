import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_components/pip_services3_components.dart';
import 'package:pip_services3_mysql/src/connect/MySqlConnection.dart';

/// Creates MySql components by their descriptors.
///
/// See [Factory]
/// See [MySqlConnection]
class DefaultMySqlFactory extends Factory {
  static var MySqlConnectionDescriptor =
      Descriptor("pip-services", "connection", "mysql", "*", "1.0");

  ///  Create a new instance of the factory.
  DefaultMySqlFactory() : super() {
    this.registerAsType(
        DefaultMySqlFactory.MySqlConnectionDescriptor, MySqlConnection);
  }
}

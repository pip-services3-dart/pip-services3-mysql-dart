import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_components/pip_services3_components.dart';

/// Helper class that resolves MySQL connection and credential parameters,
/// validates them and generates a connection URI.
///
/// It is able to process multiple connections to MySQL cluster nodes.
///
/// ### Configuration parameters ###
///
/// - [connection(s)]:
///   - [discovery_key]:               (optional) a key to retrieve the connection from [IDiscovery]
///   - [host]:                        host name or IP address
///   - [port]:                        port number (default: 27017)
///   - [database]:                    database name
///   - [uri]:                         resource URI or connection string with all parameters in it
/// - [credential(s)]:
///   - [store_key]:                   (optional) a key to retrieve the credentials from [ICredentialStore]
///   - [username]:                    user name
///   - [password]:                    user password
///
/// ### References ###
///
/// - \*:discovery:\*:\*:1.0            (optional) [IDiscovery] services
/// - \*:credential-store:\*:\*:1.0     (optional) Credential stores to resolve credentials
///
class MySqlConnectionResolver implements IReferenceable, IConfigurable {
  // The connections resolver.
  ConnectionResolver connectionResolver_ = ConnectionResolver();
  // The credentials resolver.
  CredentialResolver credentialResolver_ = CredentialResolver();

  /// Configures component by passing configuration parameters.
  ///
  /// - [config]    configuration parameters to be set.
  @override
  void configure(ConfigParams config) {
    connectionResolver_.configure(config);
    credentialResolver_.configure(config);
  }

  /// Sets references to dependent components.
  ///
  /// - [references] 	references to locate the component dependencies.
  @override
  void setReferences(IReferences references) {
    connectionResolver_.setReferences(references);
    credentialResolver_.setReferences(references);
  }

  void _validateConnection(String? correlationId, ConnectionParams connection) {
    var uri = connection.getUri();
    if (uri != null) return;

    var host = connection.getHost();
    if (host == null) {
      throw new ConfigException(
          correlationId, "NO_HOST", "Connection host is not set");
    }

    var port = connection.getPort();
    if (port == 0) {
      throw new ConfigException(
          correlationId, "NO_PORT", "Connection port is not set");
    }

    var database = connection.getAsNullableString("database");
    if (database == null) {
      throw new ConfigException(
          correlationId, "NO_DATABASE", "Connection database is not set");
    }
  }

  void _validateConnections(
      String? correlationId, List<ConnectionParams>? connections) {
    if (connections == null || connections.length == 0) {
      throw new ConfigException(
          correlationId, "NO_CONNECTION", "Database connection is not set");
    }

    for (var connection in connections) {
      _validateConnection(correlationId, connection);
    }
  }

  String _composeUri(
      List<ConnectionParams> connections, CredentialParams? credential) {
    // If there is a uri then return it immediately
    for (var connection in connections) {
      var uri = connection.getUri();
      if (uri != null) return uri;
    }

    // Define hosts
    var hosts = '';
    for (var connection in connections) {
      var host = connection.getHost();
      var port = connection.getPort();

      if (hosts.length > 0) {
        hosts += ',';
      }
      hosts += host! + (port == null ? '' : ':' + port.toString());
    }

    // Define database
    String? database = null;
    for (var connection in connections) {
      database = database ?? connection.getAsNullableString("database");
    }
    if (database != null && database.isNotEmpty) {
      database = '/' + database;
    }

    // Define authentication part
    var auth = '';
    if (credential != null) {
      var username = credential.getUsername();
      if (username != null) {
        var password = credential.getPassword();
        if (password != null) {
          auth = username + ':' + password + '@';
        } else {
          auth = username + '@';
        }
      }
    }

    // Define additional parameters parameters
    var options = ConfigParams.mergeConfigs(connections);
    if (credential != null) {
      options = options.override(credential);
    }
    options.remove('uri');
    options.remove('host');
    options.remove('port');
    options.remove('database');
    options.remove('username');
    options.remove('password');
    var params = '';
    var keys = options.getKeys();
    for (var key in keys) {
      if (params.length > 0) {
        params += '&';
      }

      params += key;

      var value = options.getAsNullableString(key);
      if (value != null) {
        params += '=' + value;
      }
    }
    if (params.length > 0) {
      params = '?' + params;
    }

    // Compose uri
    var uri = "mysql://" + auth + hosts + database! + params;

    return uri;
  }

  /// Resolves MySql connection URI from connection and credential parameters.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// Return a resolved URI.
  Future<String> resolve(String? correlationId) async {
    var connections = await connectionResolver_.resolveAll(correlationId);
    // Validate connections
    _validateConnections(correlationId, connections);

    var credential = await credentialResolver_.lookup(correlationId);
    // Credentials are not validated right now

    var uri = _composeUri(connections, credential);
    return uri;
  }
}

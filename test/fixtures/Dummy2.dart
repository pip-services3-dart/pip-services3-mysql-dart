import 'package:pip_services3_commons/pip_services3_commons.dart';

class Dummy2 implements IIdentifiable<int>, ICloneable {
  @override
  int? id;
  String? key;
  String? content;

  Dummy2();

  Dummy2.from(this.id, this.key, this.content);

  Map<String, dynamic> toJson() {
    return <String, dynamic>{'id': id, 'key': key, 'content': content};
  }

  void fromJson(Map<String, dynamic> json) {
    id = json['id'];
    key = json['key'];
    content = json['content'];
  }

  @override
  Dummy2 clone() {
    return Dummy2.from(id, key, content);
  }
}

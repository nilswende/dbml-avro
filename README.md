# DBML-to-Avro-Translator

Translates tables and enums defined with Database Markup Language (DBML) to [Apache Avro](https://avro.apache.org) schemas in JSON format.

Using Java 17.

Example usage:
```java
import com.wn.dbml.avro.Config;
import com.wn.dbml.avro.DbmlAvroTranslator;
import com.wn.dbml.avro.DbmlAvroTranslator.Result;

class Example {
  public static void main(String[] args) {
    var dbml = """
        Table User {
          id integer [not null]
          name varchar
        }""";
    var config = Config.builder().setNamespace("com.example").build();
    // translate the DBML to Avro schemas
    var translated = new DbmlAvroTranslator(config).translate(dbml);
    // process the Avro schema JSONs
    translated.stream()
        .map(Result::schema)
        .forEach(System.out::println);
  }
}
```

The resulting schema will look like this:
```json
{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": ["string", "null"]}
  ]
}
```

Maven dependency:
```xml
<dependency>
    <groupId>io.github.nilswende</groupId>
    <artifactId>dbml-avro</artifactId>
    <version>1.1.0</version>
</dependency>
```

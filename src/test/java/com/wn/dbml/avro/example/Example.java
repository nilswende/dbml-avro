package com.wn.dbml.avro.example;

import com.wn.dbml.avro.Config;
import com.wn.dbml.avro.DbmlAvroTranslator;
import com.wn.dbml.avro.DbmlAvroTranslator.Result;

public class Example {
	public static void main(String[] args) {
		var dbml = """
				Table User {
				  id integer [not null]
				  name varchar
				}""";
		var config = Config.builder().setNamespace("com.example").build();
		// translate your DBML to Avro schemas
		var translated = new DbmlAvroTranslator(config).translate(dbml);
		// process the Avro schema JSONs
		translated.stream()
				.map(Result::schema)
				.forEach(System.out::println);
	}
}

package com.wn.dbml.avro;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class DbmlAvroTranslatorTest {
	static Map<String, String> toMap(List<DbmlAvroTranslator.Result> translated) {
		return translated.stream().collect(Collectors.toMap(DbmlAvroTranslator.Result::name, DbmlAvroTranslator.Result::schema));
	}
	
	@Test
	void translateTable() {
		var dbml = """
				Table User as "U" {
					name varchar(255) [not null]
					favorite_number integer [note: "should be prime"]
					favorite_color varchar
					note : "table of users"
				}
				""";
		var expected = """
				{
				  "type": "record",
				  "name": "User",
				  "namespace": "com.example",
				  "doc": "table of users",
				  "aliases": ["U"],
				  "fields": [
				    {"name": "name", "type": "string"},
				    {"name": "favorite_number", "doc": "should be prime", "type": ["int", "null"]},
				    {"name": "favorite_color", "type": ["string", "null"]}
				  ]
				}
				""";
		var translated = new DbmlAvroTranslator(Config.builder().setNamespace("com.example").build()).translate(dbml);
		assertEquals(1, translated.size());
		var map = toMap(translated);
		var user = map.get("User");
		assertNotNull(user);
		assertEquals(expected, user);
	}
	
	@Test
	void translateEnum() {
		var dbml = """
				Enum Suit {
					SPADES
					HEARTS
					DIAMONDS
					CLUBS
				}
				""";
		var expected = """
				{
				  "type": "enum",
				  "name": "Suit",
				  "namespace": "com.example",
				  "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
				}
				""";
		var translated = new DbmlAvroTranslator(Config.builder().setNamespace("com.example").build()).translate(dbml);
		assertEquals(1, translated.size());
		var map = toMap(translated);
		var suit = map.get("Suit");
		assertNotNull(suit);
		assertEquals(expected, suit);
	}
	
	@Test
	void failName() {
		var dbml = """
				Table Üser {
					name varchar
				}
				""";
		assertThrows(IllegalArgumentException.class, () ->
				new DbmlAvroTranslator(Config.builder().build()).translate(dbml));
	}
	
	@Test
	void failNamespace() {
		var dbml = """
				Table User {
					name varchar
				}
				""";
		assertThrows(IllegalArgumentException.class, () ->
				new DbmlAvroTranslator(Config.builder().setNamespace("com.exämple").build()).translate(dbml));
	}
}
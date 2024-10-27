package com.wn.dbml.avro;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class DbmlAvroTranslatorTest {
	static Map<String, String> toMap(List<DbmlAvroTranslator.Result> translated) {
		return translated.stream().collect(Collectors.toMap(DbmlAvroTranslator.Result::name, DbmlAvroTranslator.Result::schema));
	}
	
	static void validateSchemas(List<DbmlAvroTranslator.Result> schemas) {
		schemas.forEach(r -> assertDoesNotThrow(() -> new Schema.Parser().parse(r.schema()), r.toString()));
	}
	
	@Test
	void testValidateSchemas() {
		var invalidSchema = """
				{
				  "type": "unknown",
				  "name": "User"
				}""";
		var list = List.of(new DbmlAvroTranslator.Result("User", invalidSchema));
		assertThrows(AssertionFailedError.class, () -> validateSchemas(list));
	}
	
	@Test
	void translateExample() {
		var dbml = """
				Table User {
				  id integer [not null]
				  name varchar
				}""";
		var expected = """
				{
				  "type": "record",
				  "name": "User",
				  "fields": [
				    {"name": "id", "type": "int"},
				    {"name": "name", "type": ["string", "null"]}
				  ]
				}""";
		var translated = new DbmlAvroTranslator(Config.builder().build()).translate(new StringReader(dbml));
		validateSchemas(translated);
		assertEquals(1, translated.size());
		var map = toMap(translated);
		var user = map.get("User");
		assertNotNull(user);
		assertEquals(expected, user);
	}
	
	@Test
	void translateTable() {
		var dbml = """
				Table User as "U" {
					name varchar(255) [not null]
					favorite_number integer [note: "should be prime"]
					favorite_color varchar
					note : "table of users"
				}""";
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
				}""";
		var translated = new DbmlAvroTranslator(Config.builder().setNamespace("com.example").build()).translate(dbml);
		validateSchemas(translated);
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
				}""";
		var expected = """
				{
				  "type": "enum",
				  "name": "Suit",
				  "namespace": "com.example",
				  "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
				}""";
		var translated = new DbmlAvroTranslator(Config.builder().setNamespace("com.example").build()).translate(dbml);
		validateSchemas(translated);
		assertEquals(1, translated.size());
		var map = toMap(translated);
		var suit = map.get("Suit");
		assertNotNull(suit);
		assertEquals(expected, suit);
	}
	
	@Test
	void translateBoth() {
		var dbml = """
				Table User {
					name varchar [not null]
					favorite_suit Suit [not null]
				}
				
				Enum Suit {
					SPADES
					HEARTS
					DIAMONDS
					CLUBS
				}""";
		var expected = List.of("""
				{
				  "type": "record",
				  "name": "User",
				  "fields": [
				    {"name": "name", "type": "string"},
				    {"name": "favorite_suit", "type": {
				      "type": "enum",
				      "name": "Suit",
				      "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
				    }}
				  ]
				}""", """
				{
				  "type": "enum",
				  "name": "Suit",
				  "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
				}""");
		var translated = new DbmlAvroTranslator(Config.builder().build()).translate(dbml);
		validateSchemas(translated);
		assertEquals(2, translated.size());
		var map = toMap(translated);
		var user = map.get("User");
		assertNotNull(user);
		assertEquals(expected.get(0), user);
		var suit = map.get("Suit");
		assertNotNull(suit);
		assertEquals(expected.get(1), suit);
	}
	
	@Test
	void translateBothWithMultipleEnumRef() {
		var dbml = """
				Table User {
					name varchar [not null]
					favorite_suit Suit [not null]
					least_favorite_suit Suit [not null]
				}
				
				Table User2 {
					name varchar [not null]
					favorite_suit Suit
					least_favorite_suit Suit
				}
				
				Enum Suit {
					SPADES
					HEARTS
					DIAMONDS
					CLUBS
				}""";
		var expected = List.of("""
				{
				  "type": "record",
				  "name": "User",
				  "fields": [
				    {"name": "name", "type": "string"},
				    {"name": "favorite_suit", "type": {
				      "type": "enum",
				      "name": "Suit",
				      "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
				    }},
				    {"name": "least_favorite_suit", "type": "Suit"}
				  ]
				}""", """
				{
				  "type": "record",
				  "name": "User2",
				  "fields": [
				    {"name": "name", "type": "string"},
				    {"name": "favorite_suit", "type": [{
				      "type": "enum",
				      "name": "Suit",
				      "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
				    }, "null"]},
				    {"name": "least_favorite_suit", "type": ["Suit", "null"]}
				  ]
				}""", """
				{
				  "type": "enum",
				  "name": "Suit",
				  "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
				}""");
		var translated = new DbmlAvroTranslator(Config.builder().build()).translate(dbml);
		validateSchemas(translated);
		assertEquals(3, translated.size());
		var map = toMap(translated);
		var user = map.get("User");
		assertNotNull(user);
		assertEquals(expected.get(0), user);
		var user2 = map.get("User2");
		assertNotNull(user2);
		assertEquals(expected.get(1), user2);
		var suit = map.get("Suit");
		assertNotNull(suit);
		assertEquals(expected.get(2), suit);
	}
	
	@Test
	void failName() {
		var dbml = """
				Table Üser {
					name varchar
				}""";
		assertThrows(IllegalArgumentException.class, () ->
				new DbmlAvroTranslator(Config.builder().build()).translate(dbml));
	}
	
	@Test
	void failNamespace() {
		var dbml = """
				Table User {
					name varchar
				}""";
		assertThrows(IllegalArgumentException.class, () ->
				new DbmlAvroTranslator(Config.builder().setNamespace("com.exämple").build()).translate(dbml));
	}
}
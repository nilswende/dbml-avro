package com.wn.dbml.avro;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TypeMapperTest {
	@Test
	void mapPrimitiveType() {
		var mapper = new TypeMapper(Config.builder().build());
		var actual = mapper.map("integer");
		assertEquals("int", actual);
	}
	
	@Test
	void mapLogicalType() {
		var mapper = new TypeMapper(Config.builder().build());
		var actual = mapper.map("date");
		assertEquals("{\"type\": \"int\", \"logicalType\": \"date\"}", actual);
	}
	
	@Test
	void mapDecimal() {
		var mapper = new TypeMapper(Config.builder().build());
		var actual = mapper.map("numeric(18, 3)");
		assertEquals("{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 18, \"scale\": 3}", actual);
	}
	
	@Test
	void failDecimalPrecision() {
		var mapper = new TypeMapper(Config.builder().build());
		assertThrows(IllegalArgumentException.class, () -> mapper.map("decimal"));
	}
	
	@Test
	void mapDuration() {
		var mapper = new TypeMapper(Config.builder().build());
		var actual = mapper.map("duration");
		assertEquals("{\"type\": \"fixed\", \"logicalType\": \"duration\", \"size\": 12}", actual);
	}
	
	@Test
	void mapCustomType() {
		var mapper = new TypeMapper(Config.builder().addTypeMapping("int","int8").build());
		var actual = mapper.map("int8");
		assertEquals("int", actual);
	}
	
	@Test
	void failMissingType() {
		var mapper = new TypeMapper(Config.builder().build());
		assertThrows(IllegalArgumentException.class, () -> mapper.map("json"));
	}
}
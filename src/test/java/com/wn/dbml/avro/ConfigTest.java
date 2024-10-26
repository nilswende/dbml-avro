package com.wn.dbml.avro;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigTest {
	@Test
	void setMappings() {
		var config = Config.builder().setTypeMappings(Map.of()).build();
		assertTrue(config.typeMappings().isEmpty());
	}
	
	@Test
	void setScale() {
		var config = Config.builder().setDefaultScale(3).build();
		assertEquals(3, config.defaultScale());
	}
	
	@Test
	void failScaleNegative() {
		assertThrows(IllegalArgumentException.class, () ->Config.builder().setDefaultScale(-1).build());
	}
}
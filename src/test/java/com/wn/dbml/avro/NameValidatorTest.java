package com.wn.dbml.avro;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NameValidatorTest {
	@ParameterizedTest
	@MethodSource
	void isValid(String input, Boolean expected) {
		var validator = new NameValidator();
		var actual = validator.isValid(input);
		assertEquals(expected, actual);
	}
	
	static Stream<Arguments> isValid() {
		return Stream.of(
				arguments("", false),
				arguments(".", false),
				arguments("9", false),
				arguments("_", true),
				arguments("a", true),
				arguments("a.", false),
				arguments("a9", true),
				arguments("a_", true),
				arguments("a_b", true)
		);
	}
}
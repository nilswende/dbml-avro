package com.wn.dbml.avro;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NamespaceValidatorTest {
	@ParameterizedTest
	@MethodSource
	void isValid(String input, Boolean expected) {
		var validator = new NamespaceValidator(new NameValidator());
		var actual = validator.isValid(input);
		assertEquals(expected, actual);
	}
	
	static Stream<Arguments> isValid() {
		return Stream.of(
				arguments("", true),
				arguments(".", false),
				arguments(".a", false),
				arguments("a.", false),
				arguments("a.b", true)
		);
	}
}
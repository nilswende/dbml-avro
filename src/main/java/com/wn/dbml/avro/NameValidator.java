package com.wn.dbml.avro;

import java.util.regex.Pattern;

/**
 * Validates a name.
 */
public class NameValidator {
	private static final Pattern PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
	
	public boolean isValid(String name) {
		return PATTERN.matcher(name).matches();
	}
}

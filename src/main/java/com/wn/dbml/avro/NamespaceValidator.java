package com.wn.dbml.avro;

import java.util.Arrays;

/**
 * Validates a namespace.
 */
public class NamespaceValidator {
	private final NameValidator nameValidator;
	
	public NamespaceValidator(NameValidator nameValidator) {
		this.nameValidator = nameValidator;
	}
	
	public boolean isValid(String namespace) {
		return namespace.isEmpty() || Arrays.stream(namespace.split("\\.", -1)).allMatch(nameValidator::isValid);
	}
}

package com.wn.dbml.avro;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Map.entry;

/**
 * Configuration for the DBML-to-Avro-Translator.
 *
 * @param namespace    The namespace of the generated Avro schemas
 * @param typeMappings Type mappings from Avro types to sets of DBML types.
 *                     A DBML column type matches an Avro type if it starts with any of the defined DBML types.
 * @param defaultScale The scale to be used for decimals without an explicitly specified scale.
 * @see #builder()
 */
public record Config(
		String namespace,
		Map<String, Set<String>> typeMappings,
		int defaultScale) {
	public Config(String namespace, Map<String, Set<String>> typeMappings, int defaultScale) {
		this.namespace = namespace;
		this.typeMappings = typeMappings.entrySet().stream()
				.map(e -> entry(normalize(e.getKey()), e.getValue().stream().map(this::normalize).collect(Collectors.toSet())))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		this.defaultScale = defaultScale;
	}
	
	/**
	 * Create a config builder.
	 */
	public static Builder builder() {
		return new Builder();
	}
	
	public String normalize(String type) {
		return type.toLowerCase(Locale.ROOT);
	}
	
	public static class Builder {
		/**
		 * Default mapping from DBML types to Avro types.
		 */
		public static final Map<String, Set<String>> DEFAULT_TYPE_MAPPINGS = Map.ofEntries(
				entry("null", Set.of()),
				entry("boolean", Set.of("bool")),
				entry("int", Set.of("int")),
				entry("long", Set.of("long", "bigint")),
				entry("float", Set.of("float")),
				entry("double", Set.of("double")),
				entry("bytes", Set.of("blob", "binary", "bytea", "varbinary")),
				entry("string", Set.of("varchar", "char", "clob", "text", "string")),
				entry("decimal", Set.of("decimal", "numeric")),
				entry("uuid", Set.of("uuid")),
				entry("date", Set.of("date")),
				entry("time-micros", Set.of("time")),
				entry("timestamp-micros", Set.of("timestamp", "datetime")),
				entry("local-timestamp-micros", Set.of("timestamp with time zone", "timestamptz")),
				entry("duration", Set.of("duration"))
		);
		/**
		 * Avro's default scale is 0.
		 */
		public static final int DEFAULT_SCALE = 0;
		
		private String namespace = "";
		private Map<String, Set<String>> typeMappings = DEFAULT_TYPE_MAPPINGS;
		private boolean mutableMappings;
		private int defaultScale = DEFAULT_SCALE;
		
		/**
		 * Set the namespace for all translated schemas.
		 */
		public Builder setNamespace(String namespace) {
			this.namespace = namespace;
			return this;
		}
		
		/**
		 * Set the mappings from DBML types to Avro types.
		 *
		 * @see #DEFAULT_TYPE_MAPPINGS
		 */
		public Builder setTypeMappings(Map<String, Set<String>> typeMappings) {
			this.typeMappings = typeMappings;
			return this;
		}
		
		/**
		 * Add a mapping from a DBML type to an Avro type.
		 *
		 * @see #DEFAULT_TYPE_MAPPINGS
		 */
		public Builder addTypeMapping(String avroType, String dbmlType) {
			if (!mutableMappings) {
				typeMappings = new HashMap<>(typeMappings.entrySet().stream()
						.map(e -> entry(e.getKey(), new HashSet<>(e.getValue())))
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
				mutableMappings = true;
			}
			typeMappings.computeIfAbsent(avroType, x -> new HashSet<>()).add(dbmlType);
			return this;
		}
		
		/**
		 * Set the scale to be used for decimals without an explicitly specified scale.
		 *
		 * @see #DEFAULT_SCALE
		 */
		public Builder setDefaultScale(int defaultScale) {
			if (defaultScale < 0) throw new IllegalArgumentException("Scale must be zero or a positive integer");
			this.defaultScale = defaultScale;
			return this;
		}
		
		public Config build() {
			return new Config(namespace, typeMappings, defaultScale);
		}
	}
}

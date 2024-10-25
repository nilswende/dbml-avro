package com.wn.dbml.avro;

import java.util.Map;
import java.util.regex.Pattern;

public class TypeMapper {
	private static final Pattern DECIMAL_ARGS = Pattern.compile("\\(\\s*(\\d+)\\s*,?\\s*(\\d*)\\s*\\)");
	private static final Map<String, String> LOGICAL_TYPES = Map.of(
			"decimal", "bytes",
			"uuid", "string",
			"date", "int",
			"time-millis", "int",
			"time-micros", "long",
			"timestamp-millis", "long",
			"timestamp-micros", "long",
			"local-timestamp-millis", "long",
			"local-timestamp-micros", "long",
			"duration", "fixed"
	);
	private final Config config;
	
	public TypeMapper(Config config) {
		this.config = config;
	}
	
	public String map(String columnType) {
		var type = config.normalize(columnType);
		var avroType = config.typeMappings().entrySet().stream()
				.filter(e -> e.getValue().stream().anyMatch(type::startsWith))
				.findAny()
				.map(Map.Entry::getKey)
				.orElseThrow(() -> new IllegalArgumentException("Unmapped type: " + columnType));
		if (LOGICAL_TYPES.containsKey(avroType)) {
			return "{\"type\": \"%s\", \"logicalType\": \"%s\"%s}"
					.formatted(LOGICAL_TYPES.get(avroType), avroType, getAdditionalAttributes(columnType, avroType));
		}
		return avroType;
	}
	
	private String getAdditionalAttributes(String columnType, String avroType) {
		if (avroType.equals("decimal")) {
			var matcher = DECIMAL_ARGS.matcher(columnType);
			if (matcher.find()) {
				var precision = matcher.group(1);
				var group2 = matcher.group(2);
				var scale = group2.isEmpty() ? "0" : group2;
				return ", \"precision\": %s, \"scale\": %s".formatted(precision, scale);
			} else {
				throw new IllegalArgumentException("Unspecified precision of type decimal");
			}
		} else if (avroType.equals("duration")) {
			return ", \"size\": 12";
		}
		return "";
	}
}

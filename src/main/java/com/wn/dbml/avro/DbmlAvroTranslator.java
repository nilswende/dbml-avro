package com.wn.dbml.avro;

import com.wn.dbml.compiler.DbmlParser;
import com.wn.dbml.model.Column;
import com.wn.dbml.model.ColumnSetting;
import com.wn.dbml.model.Database;
import com.wn.dbml.model.Enum;
import com.wn.dbml.model.EnumValue;
import com.wn.dbml.model.Table;

import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Translates DBML to Apache Avro schemas in JSON format.
 */
public class DbmlAvroTranslator {
	private final Config config;
	private final NameValidator nameValidator;
	private final NamespaceValidator namespaceValidator;
	private final TypeMapper typeMapper;
	
	/**
	 * Default constructor.
	 *
	 * @param config Config
	 */
	public DbmlAvroTranslator(Config config) {
		this(config, new NameValidator(), new NamespaceValidator(new NameValidator()), new TypeMapper(config));
	}
	
	/**
	 * Constructor for dependency injection.
	 */
	public DbmlAvroTranslator(Config config, NameValidator nameValidator, NamespaceValidator namespaceValidator, TypeMapper typeMapper) {
		this.config = config;
		this.nameValidator = nameValidator;
		this.namespaceValidator = namespaceValidator;
		this.typeMapper = typeMapper;
	}
	
	/**
	 * Translates DBML to Avro schemas.
	 *
	 * @param dbml a DBML string
	 * @return the result list
	 */
	public List<Result> translate(String dbml) {
		return translate(DbmlParser.parse(dbml));
	}
	
	/**
	 * Translates DBML to Avro schemas.
	 *
	 * @param dbml a DBML reader
	 * @return the result list
	 */
	public List<Result> translate(Reader dbml) {
		return translate(DbmlParser.parse(dbml));
	}
	
	private List<Result> translate(Database database) {
		var enums = database.getSchemas().stream()
				.flatMap(schema -> schema.getEnums().stream().map(this::translate))
				.toList();
		var tables = database.getSchemas().stream()
				.flatMap(schema -> schema.getTables().stream().map(table -> translate(table, enums)))
				.toList();
		return Stream.concat(tables.stream(), enums.stream()).toList();
	}
	
	private Result translate(Table table, List<Result> enums) {
		var name = table.getName();
		validateName(name);
		var sw = new StringWriter();
		try (var pw = new PrintWriter(sw)) {
			pw.println("{");
			pw.println("  \"type\": \"record\",");
			pw.printf("  \"%s\": \"%s\"", "name", name);
			var namespace = config.namespace();
			if (namespace != null) {
				validateNamespace(namespace);
				pw.printf(",%n  \"%s\": \"%s\"", "namespace", namespace);
			}
			var doc = table.getNote();
			if (doc != null) {
				pw.printf(",%n  \"%s\": \"%s\"", "doc", doc);
			}
			var alias = table.getAlias();
			if (alias != null) {
				pw.printf(",%n  \"%s\": [\"%s\"]", "aliases", alias);
			}
			pw.printf(",%n  \"%s\": [%n", "fields");
			appendFields(table, enums, pw);
			pw.printf("%n  ]%n");
			pw.print("}");
		}
		return new Result(name, sw.toString());
	}
	
	private void appendFields(Table table, List<Result> enums, PrintWriter pw) {
		for (var iterator = table.getColumns().iterator(); iterator.hasNext(); ) {
			var column = iterator.next();
			appendField(column, enums, pw);
			if (iterator.hasNext()) {
				pw.println(",");
			}
		}
	}
	
	private void appendField(Column column, List<Result> enums, PrintWriter pw) {
		var name = column.getName();
		validateName(name);
		pw.printf("    {\"%s\": \"%s\"", "name", name);
		var doc = column.getNote();
		if (doc != null) {
			pw.printf(", \"%s\": \"%s\"", "doc", doc);
		}
		pw.printf(", \"%s\": ", "type");
		var enumType = enums.stream().filter(r -> r.name().equals(column.getType())).findAny().map(Result::schema);
		if (enumType.isPresent()) {
			pw.printf("%s", enumType.get().indent(4).trim());
		} else {
			var type = typeMapper.map(column.getType());
			if (column.getSettings().containsKey(ColumnSetting.NOT_NULL)) {
				pw.printf("\"%s\"", type);
			} else {
				pw.printf("[\"%s\", \"null\"]", type);
			}
		}
		pw.print("}");
	}
	
	private Result translate(Enum anEnum) {
		var name = anEnum.getName();
		validateName(name);
		var sw = new StringWriter();
		try (var pw = new PrintWriter(sw)) {
			pw.println("{");
			pw.println("  \"type\": \"enum\",");
			pw.printf("  \"%s\": \"%s\"", "name", name);
			var namespace = config.namespace();
			if (namespace != null && !namespace.isBlank()) {
				validateNamespace(namespace);
				pw.printf(",%n  \"%s\": \"%s\"", "namespace", namespace);
			}
			anEnum.getValues().forEach(v -> validateName(v.getName()));
			var symbols = anEnum.getValues().stream()
					.map(EnumValue::getName)
					.collect(Collectors.joining("\", \"", "\"", "\""));
			pw.printf(",%n  \"%s\": [%s]%n", "symbols", symbols);
			pw.print("}");
		}
		return new Result(name, sw.toString());
	}
	
	private void validateName(String name) {
		if (!nameValidator.isValid(name)) {
			throw new IllegalArgumentException("Invalid name: " + name);
		}
	}
	
	private void validateNamespace(String namespace) {
		if (!namespaceValidator.isValid(namespace)) {
			throw new IllegalArgumentException("Invalid namespace: " + namespace);
		}
	}
	
	/**
	 * Contains the result of a translation.
	 *
	 * @param name   The name of the translated table or enum.
	 * @param schema The resulting Avro schema in JSON format.
	 */
	public record Result(
			String name,
			String schema
	) {
	}
}

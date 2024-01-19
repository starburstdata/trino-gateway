package io.trino.gateway.ha.router;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import io.trino.gateway.ha.config.ProcessedRequestConfig;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.ShowColumns;
import io.trino.sql.tree.ShowSchemas;
import io.trino.sql.tree.ShowTables;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.javalite.common.Base64;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

@Slf4j
public class ProcessedRequest
{
    private String body;
    private String queryType;
    Set<QualifiedName> tables = new HashSet<>();
    String defaultCatalog;
    String defaultSchema;
    Set<String> catalogs = new HashSet<>();
    Set<String> schemas = new HashSet<>();
    Set<String> catalogSchemas = new HashSet<>();
    Optional<String> user;



    public ProcessedRequest(HttpServletRequest request, ProcessedRequestConfig config)
    {
        defaultCatalog = Objects.requireNonNullElse(request.getHeader("X-Trino-Catalog"), "__UNSET__");
        defaultSchema = Objects.requireNonNullElse(request.getHeader("X-Trino-Schema"), "__UNSET__"); //TODO
        user = extractUser(request, config.getTokenUserField());

        try (BufferedReader reader = request.getReader()) {
            if (reader == null) {
                log.warn("HTTP request returned null reader");
                body = "";
                return;
            }
            reader.mark(config.getMaxBodySize());
            body = CharStreams.toString(request.getReader());
            reader.reset();

            SqlParser parser = new SqlParser();
            Statement statement = parser.createStatement(body, new ParsingOptions(AS_DECIMAL));
            queryType = statement.getClass().getSimpleName();
            getTables(statement, tables);

            catalogs = tables.stream()
                    .map(qualifiedName -> qualifiedName.getParts().size() == 3 ? qualifiedName.getParts().get(0) : defaultCatalog)
                    .collect(Collectors.toSet());
            schemas = tables.stream()
                    .map(qualifiedName ->  qualifiedName.getParts().size() > 1 ? qualifiedName.getParts().get(3 - qualifiedName.getParts().size()) : defaultSchema)
                    .collect(Collectors.toSet());
            catalogSchemas = tables.stream()
                    .map(qualifiedName -> switch (qualifiedName.getParts().size()) {
                                case 1:
                                    yield  defaultCatalog + "." + defaultSchema;
                                case 2:
                                    yield  defaultCatalog + "." + qualifiedName.getParts().get(0);
                                case 3:
                                default:
                                    yield qualifiedName.getParts().get(0) + "." + qualifiedName.getParts().get(1);
                            })
                    .collect(Collectors.toSet());
        }
        catch (IOException e) {
            log.warn("Error extracting request body for rules processing: " + e.getMessage());
        }
    }

    private void getTables(Node node, Set<QualifiedName> tables)
    {
        if (node.getClass() == ShowColumns.class ) {
            tables.add(((ShowColumns) node).getTable());
        }
        if (node.getClass() == ShowSchemas.class ) { //TODO: be less lazy and pass a separate container for schemas
            tables.add(
                    QualifiedName.of (
                            ((ShowSchemas) node).getCatalog().orElse(new Identifier("__NONE__")).getValue(),
                            "__NONE__",
                            "__NONE__"));
        }
        if (node.getClass() == ShowTables.class ) { //TODO: be less lazy
            Optional<QualifiedName> schemaOptional = ((ShowTables) node).getSchema();
            schemaOptional.ifPresent( schema ->
                    tables.add(
                            QualifiedName.of (
                                    schema.getParts().get(0),
                                    schema.getParts().get(1),
                                    "__NONE__")));
        }
        if (node.getClass() == Table.class) {
            tables.add(((Table) node).getName());
        }
        for (Node child : node.getChildren()) {
            getTables(child, tables);
        }
    }

    private Optional<String> extractUser(HttpServletRequest request, String userField)
    {
        String header;
        header = request.getHeader("X-Trino-User");
        if (header != null) {
            log.debug("Extracted X-Trino-User");
            return Optional.of(header);
        }
        header = request.getHeader("Authorization");
        if (header != null) {
            if (header.contains("Basic")) {
                log.debug("Extracted user from basic auth");
                return Optional.of(new String(Base64.getDecoder().decode(header.split(" ")[2]), StandardCharsets.UTF_8).split(":")[0]);
            }
            if (header.toLowerCase().contains("bearer")) {
                log.debug("Trying to extract user from bearer token");
                int space = header.indexOf(' ');
                if ((space < 0) || !header.substring(0, space).equalsIgnoreCase("bearer")) {
                    return Optional.empty();
                }
                String token = header.substring(space + 1).trim();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = null;
                try {
                    node = mapper.readTree(token);
                    if (node.has(userField)) {
                        log.debug("Trying to extract user from json. User: " + node.get(userField).asText());
                        return Optional.of(node.get(userField).asText());
                    }
                }
                catch (JsonProcessingException e) {
                    log.warn("Could not deserialize bearer token as json: " + token);
                }
                if (header.split(".").length == 3) { //this is a JWS
                    log.debug("Trybing to extract from JWS");
                    token = header.split(".")[1];
                    try {
                        node = mapper.readTree(new String(Base64.getDecoder().decode(token)));
                        if (node.has(userField)) {
                            log.debug("Trying to extract user from JWS json. User: " + node.get(userField).asText());
                            return Optional.of(node.get(userField).asText());
                        }
                    }
                    catch (JsonProcessingException e) {
                        log.warn("Could not deserialize bearer token as json");
                    }
                }
            }
        }

        if (request.getCookies() != null) {
            log.debug("Trying to get user from cookie");
            Optional<Cookie> uiToken = Arrays.stream(request.getCookies()).
                    filter(cookie -> cookie.getName().equals("Trino-UI-Token")
                            || cookie.getName().equals("__Secure-Trino-ID-Token")).findAny();
            Optional<String> user = uiToken.map(cookie -> {
                if (cookie.getValue().split(".").length == 3) { //this is a JWS
                    log.debug("Found JWS cookie");
                    String token = cookie.getValue().split(".")[1];
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode node = mapper.readTree(new String(Base64.getDecoder().decode(token)));
                        if (node.has(userField)) {
                            log.debug("Got user from cookie");
                            return node.get(userField).asText();
                        }
                    }
                    catch (JsonProcessingException e) {
                        log.warn("Could not deserialize bearer token as json");
                    }
                }
                return null;
            });
            return user;
        }
        return Optional.empty();
    }

    public String getBody()
    {
        return body;
    }

    public String getQueryType()
    {
        return queryType;
    }

    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    public Set<QualifiedName> getTables()
    {
        return tables;
    }

    public String getDefaultCatalog()
    {
        return defaultCatalog;
    }

    public Set<String> getCatalogs()
    {
        return catalogs;
    }

    public Set<String> getSchemas()
    {
        return schemas;
    }

    public Set<String> getCatalogSchemas()
    {
        return catalogSchemas;
    }

    public Optional<String> getUser()
    {
        return user;
    }
}

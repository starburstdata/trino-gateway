package io.trino.gateway.ha.router;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.ShowColumns;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import lombok.extern.slf4j.Slf4j;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.api.RulesEngine;
import org.jeasy.rules.core.DefaultRulesEngine;
import org.jeasy.rules.mvel.MVELRuleFactory;
import org.jeasy.rules.support.reader.YamlRuleDefinitionReader;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

@Slf4j
public class RuleReloadingRoutingGroupSelector
    implements RoutingGroupSelector  {

  private RulesEngine rulesEngine = new DefaultRulesEngine();
  private MVELRuleFactory ruleFactory = new MVELRuleFactory(new YamlRuleDefinitionReader());
  private String rulesConfigPath;
  private volatile Rules rules = new Rules();
  private volatile long lastUpdatedTime;
  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

  RuleReloadingRoutingGroupSelector(String rulesConfigPath) {
    this.rulesConfigPath = rulesConfigPath;
    try {
      rules = ruleFactory.createRules(
              new FileReader(rulesConfigPath));
      BasicFileAttributes attr = Files.readAttributes(Path.of(rulesConfigPath),
              BasicFileAttributes.class);
      lastUpdatedTime = attr.lastModifiedTime().toMillis();

    } catch (Exception e) {
      log.error("Error opening rules configuration file, using "
              + "routing group header as default.", e);
    }
  }

  @Override
  public Map<String, Object>  extractInformation(HttpServletRequest request)
  {
    Map<String, Object> supplementalInformation = new HashMap<>();

    int maxBodySize = 2000000; //twice Trino default
    String body;
    try (BufferedReader reader = request.getReader()) {
      if (reader == null) {
        log.warn("HTTP request returned null reader");
        return supplementalInformation;
      }
      reader.mark(maxBodySize);
      char[] bodyArray = new char[maxBodySize];
      int bodySize = reader.read(bodyArray);
      body = new String(bodyArray, 0, bodySize);
      supplementalInformation.put("REQUEST_BODY", body);
      reader.reset();

      SqlParser parser = new SqlParser();
      Statement statement = parser.createStatement(body, new ParsingOptions(AS_DECIMAL));
      supplementalInformation.put("QUERY_TYPE", statement.getClass().getSimpleName()); //e.g. ShowTables, ShowCreate

      List<QualifiedName> tables = new ArrayList<>();
      getTables(statement, tables);

      String defaultCatalog = Objects.requireNonNullElse(request.getHeader("X-Trino-Catalog"), "__UNSET__");

      String defaultSchema = Objects.requireNonNullElse(request.getHeader("X-Trino-Schema"), "__UNSET__"); //TODO
      supplementalInformation.put("DEFAULT_CATALOG", defaultCatalog);
      supplementalInformation.put("DEFAULT_SCHEMA", defaultSchema);
      supplementalInformation.put("CATALOGS", tables.stream()
              .map(qualifiedName -> qualifiedName.getParts().size() == 3 ? qualifiedName.getParts().get(0) : defaultCatalog)
              .distinct()
              .collect(Collectors.toList()));

      supplementalInformation.put("SCHEMAS", tables.stream()
              .map(qualifiedName ->  qualifiedName.getParts().size() > 1 ? qualifiedName.getParts().get(3 - qualifiedName.getParts().size()) : defaultSchema)
              .distinct()
              .collect(Collectors.toList()));

      supplementalInformation.put("TABLES", tables.stream().map(QualifiedName::toString).distinct().collect(Collectors.toList()));

    } catch (IOException e) {
      log.warn("Error extracting request body for rules processing: " + e.getMessage());
    }

    return supplementalInformation;
  }

  private void getTables(Node node, List<QualifiedName> tables)
  {
    if (node.getClass() == ShowColumns.class ) {
      tables.add(((ShowColumns) node).getTable());
    }
    if (node.getClass() == Table.class) {
      tables.add(((Table) node).getName());
    }
    for (Node child : node.getChildren()) {
      getTables(child, tables);
    }
  }

  @Override
  public String findRoutingGroup(HttpServletRequest request) {
    try {
      BasicFileAttributes attr = Files.readAttributes(Path.of(rulesConfigPath),
              BasicFileAttributes.class);
      if (attr.lastModifiedTime().toMillis() > lastUpdatedTime) {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
          if (attr.lastModifiedTime().toMillis() > lastUpdatedTime) {
            // This check is performed again to prevent parsing the rules twice in case another
            // thread finds the condition true and acquires the lock before this one
            log.info(String.format("Updating rules to file modified at %s",
                    attr.lastModifiedTime()));
            rules = ruleFactory.createRules(
                    new FileReader(rulesConfigPath));
            lastUpdatedTime = attr.lastModifiedTime().toMillis();
          }
        } finally {
          writeLock.unlock();
        }
      }
      Map<String, Object> supplementalInformation = extractInformation(request);
      Facts facts = new Facts();
      HashMap<String, String> result = new HashMap<String, String>();
      facts.put("request", request);
      facts.put("result", result);
      facts.put("supplementalInformation", supplementalInformation);
      Lock readLock = readWriteLock.readLock();
      readLock.lock();
      try {
        rulesEngine.fire(rules, facts);
      } finally {
        readLock.unlock();
      }
      return result.get("routingGroup");

    } catch (Exception e) {
      log.error("Error opening rules configuration file, using "
              + "routing group header as default.", e);
      // Invalid rules could lead to perf problems as every thread goes into the writeLock
      // block until the issue is resolved
    }
    return request.getHeader(ROUTING_GROUP_HEADER);
  }
}

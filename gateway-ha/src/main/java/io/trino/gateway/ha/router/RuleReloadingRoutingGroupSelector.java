package io.trino.gateway.ha.router;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.gateway.ha.config.ProcessedRequestConfig;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.ShowSchemas;
import io.trino.sql.tree.ShowTables;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonParser;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.ShowColumns;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import lombok.extern.slf4j.Slf4j;
import org.javalite.common.Base64;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.api.RulesEngine;
import org.jeasy.rules.core.DefaultRulesEngine;
import org.jeasy.rules.mvel.MVELRuleFactory;
import org.jeasy.rules.support.reader.YamlRuleDefinitionReader;

import javax.swing.text.html.Option;

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
  private ProcessedRequestConfig processedRequestConfig;

  RuleReloadingRoutingGroupSelector(String rulesConfigPath, ProcessedRequestConfig processedRequestConfig) {
    this.rulesConfigPath = rulesConfigPath;
    this.processedRequestConfig = processedRequestConfig;
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
  public String findRoutingGroup(HttpServletRequest request) {
    try {
      BasicFileAttributes attr = Files.readAttributes(Path.of(rulesConfigPath),
              BasicFileAttributes.class);
      log.debug("File modified time: " + attr.lastModifiedTime() + ". lastUpdatedTime: " + lastUpdatedTime);
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
      ProcessedRequest processedRequest = new ProcessedRequest(request, processedRequestConfig);
      Facts facts = new Facts();
      HashMap<String, String> result = new HashMap<String, String>();
      facts.put("request", request);
      facts.put("result", result);
      facts.put("processedRequest", processedRequest);
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

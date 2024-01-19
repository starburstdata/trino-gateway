package io.trino.gateway.ha.router;

import javax.servlet.http.HttpServletRequest;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * RoutingGroupSelector provides a way to match an HTTP request to a Gateway routing group.
 */
public interface RoutingGroupSelector {
  String ROUTING_GROUP_HEADER = "X-Trino-Routing-Group";

  /**
   * Routing group selector that relies on the X-Trino-Routing-Group
   * header to determine the right routing group.
   */
  static RoutingGroupSelector byRoutingGroupHeader() {
    return request -> request.getHeader(ROUTING_GROUP_HEADER);
  }

  /**
   * Routing group selector that uses routing engine rules
   * to determine the right routing group.
   */
  static RoutingGroupSelector byRoutingRulesEngine(String rulesConfigPath) {
    return new RuleReloadingRoutingGroupSelector(rulesConfigPath);
  }

  /**
   * Given an HTTP request find a routing group to direct the request to. If a routing group cannot
   * be determined return null.
   */
  String findRoutingGroup(HttpServletRequest request);

  /**
   * Preprocess the request to extract useful information. This map is passed to the rules engine
   * as `supplementalInformation`.
   */
  default Map<String, Object>  extractInformation(HttpServletRequest request)
  {
    return ImmutableMap.of();
  }

  @Slf4j
  final class Logger {
  }
}

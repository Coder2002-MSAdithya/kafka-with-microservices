package jugistanbul.difc;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/** JUG pipeline is Kafka-only; no JDBC or other external sockets expected by default. */
public final class ExpectedExternalConnectionsRegistry {

  private static final Map<String, Set<String>> DEFAULTS = Map.of();

  private ExpectedExternalConnectionsRegistry() {
  }

  public static Set<String> defaultsForPrincipal(final String principal) {
    if (principal == null) {
      return Set.of();
    }
    return DEFAULTS.getOrDefault(normalizePrincipal(principal), Set.of());
  }

  private static String normalizePrincipal(final String principal) {
    if (principal == null) {
      return "";
    }
    if (principal.endsWith("-consumer")) {
      return principal.substring(0, principal.length() - "-consumer".length());
    }
    if (principal.endsWith("-producer")) {
      return principal.substring(0, principal.length() - "-producer".length());
    }
    return principal;
  }
}

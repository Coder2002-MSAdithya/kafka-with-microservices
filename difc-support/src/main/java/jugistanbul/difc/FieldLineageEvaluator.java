package jugistanbul.difc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Grantor-side evaluation of expression lineage through relational-algebra trees. */
public final class FieldLineageEvaluator {

  private FieldLineageEvaluator() {}

  public static Map<String, FieldLineage> evaluateEgressLineages(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode root, final String sinkTopic) {
    if (root == null) {
      return Map.of();
    }
    final Map<String, FieldLineage> pipeline;
    if ("sink".equals(root.getKind())) {
      if (root.getChildren().isEmpty()) {
        pipeline = passthroughTopicLineages(sinkTopic);
      } else {
        pipeline = evaluateNodeLineages(root.getChildren().get(0), sinkTopic);
      }
    } else {
      pipeline = evaluateNodeLineages(root, sinkTopic);
    }
    if (schemaNarrowingEgress(sinkTopic, pipeline.keySet())) {
      return passthroughTopicLineages(sinkTopic);
    }
    return pipeline;
  }

  public static Map<String, FieldLineage> evaluateNodeLineages(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node, final String sinkTopic) {
    if (node == null) {
      return Map.of();
    }
    return switch (node.getKind()) {
      case "scan" -> passthroughTopicLineages(node.getTopic());
      case "operator" -> evaluateOperatorLineages(node, sinkTopic);
      case "sink" -> node.getChildren().isEmpty()
          ? passthroughTopicLineages(sinkTopic)
          : evaluateNodeLineages(node.getChildren().get(0), sinkTopic);
      default -> Map.of();
    };
  }

  private static Map<String, FieldLineage> evaluateOperatorLineages(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node, final String sinkTopic) {
    final String op = node.getTopic() == null ? "" : node.getTopic();
    if (isJoinOp(op) || "merge".equals(op)) {
      return unionChildLineages(node, sinkTopic);
    }
    if (Set.of("mapvalues", "map", "flatmapvalues", "flatmap", "transform", "transformvalues", "process")
        .contains(op)) {
      if (!node.getFieldLineages().isEmpty()) {
        return toMap(node.getFieldLineages());
      }
      if (!node.getOutputFields().isEmpty()) {
        return synthesizeProjectionFromOutputNames(node.getOutputFields(), childLineages(node, sinkTopic));
      }
      return childLineages(node, sinkTopic);
    }
    if ("filter".equals(op) || "filternot".equals(op)) {
      return childLineages(node, sinkTopic);
    }
    if (Set.of("selectkey", "groupby", "groupbykey", "windowedby").contains(op)) {
      if (!node.getFieldLineages().isEmpty()) {
        return markKeyOnly(node.getFieldLineages(), childLineages(node, sinkTopic));
      }
      return childLineages(node, sinkTopic);
    }
    if (Set.of("aggregate", "reduce", "count").contains(op)) {
      if ("orders".equals(sinkTopic)) {
        return passthroughTopicLineages("orders");
      }
      return aggregateLineages(node, sinkTopic);
    }
    return childLineages(node, sinkTopic);
  }

  private static boolean isJoinOp(final String op) {
    return Set.of("join", "leftjoin", "outerjoin").contains(op);
  }

  private static Map<String, FieldLineage> aggregateLineages(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node, final String sinkTopic) {
    if (!node.getFieldLineages().isEmpty()) {
      return toMap(node.getFieldLineages());
    }
    if (!node.getOutputFields().isEmpty()) {
      final Map<String, FieldLineage> child = childLineages(node, sinkTopic);
      final Map<String, FieldLineage> out = new LinkedHashMap<>();
      final Set<String> sources = new LinkedHashSet<>();
      for (final FieldLineage lineage : child.values()) {
        sources.addAll(lineage.sourceFieldSet());
      }
      for (final String field : node.getOutputFields()) {
        out.put(field, FieldLineage.aggregateOf(field, sources, "γ(" + String.join(", ", sources) + ")"));
      }
      return out;
    }
    final Map<String, FieldLineage> child = childLineages(node, sinkTopic);
    final Set<String> sources = new LinkedHashSet<>();
    for (final FieldLineage lineage : child.values()) {
      if (lineage.sanitizationKindEnum() == FieldLineage.SanitizationKind.PASSTHROUGH) {
        sources.add(lineage.getOutputField());
      }
    }
    return Map.of("_aggregate_value", FieldLineage.aggregateOf("_aggregate_value", sources, "γ"));
  }

  private static Map<String, FieldLineage> unionChildLineages(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node, final String sinkTopic) {
    final Map<String, FieldLineage> merged = new LinkedHashMap<>();
    for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
      final Map<String, FieldLineage> branch = evaluateNodeLineages(child, sinkTopic);
      for (final Map.Entry<String, FieldLineage> entry : branch.entrySet()) {
        merged.merge(entry.getKey(), entry.getValue(), FieldLineageEvaluator::preferLeakingLineage);
      }
    }
    if ("orders".equals(sinkTopic) && isJoinOp(node.getTopic())) {
      return passthroughTopicLineages("orders");
    }
    return merged;
  }

  private static FieldLineage preferLeakingLineage(final FieldLineage left, final FieldLineage right) {
    if (!left.sanitizesSensitiveSources()) {
      return left;
    }
    if (!right.sanitizesSensitiveSources()) {
      return right;
    }
    return left;
  }

  private static Map<String, FieldLineage> childLineages(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node, final String sinkTopic) {
    if (node.getChildren().isEmpty()) {
      return Map.of();
    }
    return evaluateNodeLineages(node.getChildren().get(0), sinkTopic);
  }

  private static Map<String, FieldLineage> passthroughTopicLineages(final String topic) {
    final Set<String> fields = TopicSchemaCatalog.valueFieldsForTopic(topic);
    final Map<String, FieldLineage> lineages = new LinkedHashMap<>();
    for (final String field : fields) {
      lineages.put(field, FieldLineage.passthrough(field, FieldLineage.ValueType.UNKNOWN));
    }
    return lineages;
  }

  private static Map<String, FieldLineage> synthesizeProjectionFromOutputNames(
      final List<String> outputFields, final Map<String, FieldLineage> child) {
    final Map<String, FieldLineage> out = new LinkedHashMap<>();
    for (final String name : outputFields) {
      final FieldLineage childLineage = child.get(name);
      if (childLineage != null
          && childLineage.sanitizationKindEnum() == FieldLineage.SanitizationKind.PASSTHROUGH) {
        out.put(name, childLineage);
      } else {
        out.put(
            name,
            new FieldLineage(
                name,
                FieldLineage.ValueType.UNKNOWN,
                childLineage == null ? Set.of() : childLineage.sourceFieldSet(),
                name,
                FieldLineage.SanitizationKind.DERIVED));
      }
    }
    return out;
  }

  private static Map<String, FieldLineage> markKeyOnly(
      final List<FieldLineage> keyLineages, final Map<String, FieldLineage> child) {
    final Map<String, FieldLineage> out = new LinkedHashMap<>(child);
    for (final FieldLineage key : keyLineages) {
      out.put(
          key.getOutputField(),
          new FieldLineage(
              key.getOutputField(),
              key.valueTypeEnum(),
              key.sourceFieldSet(),
              key.getExpression(),
              FieldLineage.SanitizationKind.KEY_ONLY));
    }
    return out;
  }

  private static Map<String, FieldLineage> toMap(final List<FieldLineage> lineages) {
    final Map<String, FieldLineage> map = new LinkedHashMap<>();
    for (final FieldLineage lineage : lineages) {
      if (lineage != null && lineage.getOutputField() != null && !lineage.getOutputField().isEmpty()) {
        map.put(lineage.getOutputField(), lineage);
      }
    }
    return map;
  }

  private static boolean schemaNarrowingEgress(final String sinkTopic, final Set<String> pipelineFields) {
    if (!TopicSchemaCatalog.isKnownTopic(sinkTopic) || pipelineFields.isEmpty()) {
      return false;
    }
    final Set<String> sinkSchema = TopicSchemaCatalog.valueFieldsForTopic(sinkTopic);
    if (sinkSchema.isEmpty() || sinkSchema.equals(pipelineFields)) {
      return false;
    }
    // Narrow only when the pipeline schema is a superset of the sink (intentional projection).
    if (!pipelineFields.containsAll(sinkSchema)) {
      return false;
    }
    final Set<String> retainedOrderSensitive = new LinkedHashSet<>(pipelineFields);
    retainedOrderSensitive.retainAll(TopicSchemaCatalog.sensitiveOrderFields());
    final Set<String> sinkOrderSensitive = new LinkedHashSet<>(sinkSchema);
    sinkOrderSensitive.retainAll(TopicSchemaCatalog.sensitiveOrderFields());
    return !retainedOrderSensitive.isEmpty() && sinkOrderSensitive.isEmpty();
  }
}

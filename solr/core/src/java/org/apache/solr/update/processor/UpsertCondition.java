package org.apache.solr.update.processor;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.lucene.search.BooleanClause;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

class UpsertCondition {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Pattern ACTION_PATTERN = Pattern.compile("^(skip|insert)|(upsert|retain):(\\*|[\\w,]+)|(nullify):([\\w,]+)$");
  private static final List<String> ALL_FIELDS = Collections.singletonList("*");

  private final String name;
  private final List<FieldRule> rules;
  private final List<Action> actions;

  UpsertCondition(String name, List<FieldRule> rules, List<Action> actions) {
    this.name = name;
    this.rules = rules;
    this.actions = actions;
  }

  static List<UpsertCondition> readConditions(NamedList args) {
    List<UpsertCondition> conditions = new ArrayList<>(args.size());
    for (Map.Entry<String, ?> entry: (NamedList<?>)args) {
      String name = entry.getKey();
      Object tmp = entry.getValue();
      if (tmp instanceof NamedList) {
        NamedList<String> condition = (NamedList<String>)tmp;
        conditions.add(UpsertCondition.parse(name, condition));
      } else {
        throw new SolrException(SERVER_ERROR, tmp + " not a valid upsert condition");
      }
    }
    return conditions;
  }

  static boolean shouldInsertOrUpsert(List<UpsertCondition> conditions, SolrInputDocument oldDoc, SolrInputDocument newDoc) {
    for (UpsertCondition condition: conditions) {
      if (condition.matches(oldDoc, newDoc)) {
        log.debug("Condition {} matched, running actions", condition.getName());
        ActionType action = condition.run(oldDoc, newDoc);
        if (action == ActionType.SKIP) {
          log.debug("Condition {} matched - skipping insert", condition.getName());
          return false;
        }
        if (action == ActionType.INSERT) {
          log.debug("Condition {} matched - will insert", condition.getName());
          break;
        }
      }
    }
    return true;
  }

  static UpsertCondition parse(String name, NamedList<String> args) {
    List<FieldRule> rules = new ArrayList<>();
    List<Action> actions = new ArrayList<>();
    for (Map.Entry<String, String> entry: args) {
      String key = entry.getKey();
      if ("action".equals(key)) {
        String actionValue = entry.getValue();
        actions.add(Action.parse(actionValue));
      } else {
        BooleanClause.Occur occur;
        try {
          occur = BooleanClause.Occur.valueOf(key.toUpperCase(Locale.ROOT));
        } catch(IllegalArgumentException e) {
          throw new SolrException(SERVER_ERROR, "'" + key + "' not a valid occurence value");
        }
        String value = entry.getValue();
        rules.add(FieldRule.parse(occur, value));
      }
    }
    if (actions.isEmpty()) {
      throw new SolrException(SERVER_ERROR, "no actions defined for condition: " + name);
    }
    if (rules.isEmpty()) {
      throw new SolrException(SERVER_ERROR, "no rules specified for condition: " + name);
    }
    return new UpsertCondition(name, rules, actions);
  }

  String getName() {
    return name;
  }

  ActionType run(SolrInputDocument oldDoc, SolrInputDocument newDoc) {
    ActionType last = ActionType.INSERT;
    for (Action action: actions) {
      action.run(oldDoc, newDoc);
      last = action.type;
      if (last == ActionType.SKIP || last == ActionType.INSERT) {
        break;
      }
    }
    return last;
  }

  boolean matches(SolrInputDocument oldDoc, SolrInputDocument newDoc) {
    Docs docs = new Docs(oldDoc, newDoc);
    boolean atLeastOneMatched = false;
    for (FieldRule rule: rules) {
      boolean ruleMatched = rule.matches(docs);
      switch(rule.getOccur()) {
        case MUST:
          if (!ruleMatched) {
            return false;
          }
          atLeastOneMatched = true;
          break;
        case MUST_NOT:
          if (ruleMatched) {
            return false;
          }
          atLeastOneMatched = true;
          break;
        default:
          atLeastOneMatched = ruleMatched || atLeastOneMatched;
          break;
      }
    }
    return atLeastOneMatched;
  }

  enum ActionType {
    UPSERT, // copy some/all fields from the OLD doc (when they don't exist on the new doc)
    RETAIN, // copy some/all fields from the OLD doc always
    NULLIFY, // make sure specific fields are null before doc written
    INSERT, // just do a regular insert as normal
    SKIP;   // entirely skip inserting the doc
  }

  private static class Docs {
    private final SolrInputDocument oldDoc;
    private final SolrInputDocument newDoc;

    Docs(SolrInputDocument oldDoc, SolrInputDocument newDoc) {
      this.oldDoc = oldDoc;
      this.newDoc = newDoc;
    }

    SolrInputDocument getOldDoc() {
      return oldDoc;
    }

    SolrInputDocument getNewDoc() {
      return newDoc;
    }
  }

  private static class FieldRule {
    private static final Pattern RULE_CONDITION_PATTERN = Pattern.compile("^(OLD|NEW)\\.(\\*|\\w+:(?:\\w+|\\*))$");

    private final BooleanClause.Occur occur;
    private final Function<Docs, SolrInputDocument> docGetter;
    private final Predicate<SolrInputDocument> docPredicate;

    private FieldRule(BooleanClause.Occur occur, Function<Docs, SolrInputDocument> docGetter, Predicate<SolrInputDocument> docPredicate) {
      this.occur = occur;
      this.docGetter = docGetter;
      this.docPredicate = docPredicate;
    }

    static FieldRule parse(BooleanClause.Occur occur, String condition) {
      Matcher m = RULE_CONDITION_PATTERN.matcher(condition);
      if (m.matches()) {
        String doc = m.group(1);
        String predicate = m.group(2);
        Function<Docs, SolrInputDocument> docGetter;
        if (doc.equalsIgnoreCase("OLD")) {
          docGetter = Docs::getOldDoc;
        } else {
          docGetter = Docs::getNewDoc;
        }

        Predicate<SolrInputDocument> docPredicate;
        if ("*".equals(predicate)) {
          docPredicate = Objects::nonNull;
        } else {
          String[] parts = predicate.split(":");
          String field = parts[0];
          String value = parts[1];
          if ("*".equals(value)) {
            docPredicate = forField(field, Objects::nonNull);
          } else {
            docPredicate = forField(field, stringlyEquals(value));
          }
        }
        return new FieldRule(occur, docGetter, docPredicate);
      }
      throw new SolrException(SERVER_ERROR, "'" + condition + "' not a valid condition for rule");
    }

    BooleanClause.Occur getOccur() {
      return occur;
    }

    boolean matches(Docs docs) {
      SolrInputDocument doc = docGetter.apply(docs);
      return docPredicate.test(doc);
    }

    private static Predicate<Object> stringlyEquals(String value) {
      return fieldValue -> {
        if (fieldValue == null) {
          return false;
        }
        if (!(fieldValue instanceof String)) {
          return value.equals(fieldValue.toString());
        }
        return value.equals(fieldValue);
      };
    }

    private static Predicate<Map<?,?>> forAtomicUpdate(Predicate<Object> fieldPredicate) {
      return fieldValue -> {
        if (fieldValue.containsKey("set") || fieldValue.containsKey("add")) {
          return fieldValue.values().stream()
              .flatMap(updateValue -> {
                if (updateValue instanceof Collection) {
                  return ((Collection<?>)updateValue).stream();
                }
                return Stream.of(updateValue);
              })
              .anyMatch(fieldPredicate);
        }
        return false;
      };
    }

    private static Predicate<SolrInputDocument> forField(String field, Predicate<Object> fieldPredicate) {
      Predicate<Map<?,?>> atomicUpdatePredicate = forAtomicUpdate(fieldPredicate);
      Predicate<Object> predicate = fieldValue -> {
        if (fieldValue instanceof Map) {
          return atomicUpdatePredicate.test((Map<?,?>)fieldValue);
        }
        return fieldPredicate.test(fieldValue);
      };
      return doc -> {
        if (doc != null) {
          Collection<Object> values = doc.getFieldValues(field);
          if (values != null) {
            return values.stream().anyMatch(predicate);
          }
        }
        return false;
      };
    }
  }

  private static class Action {
    private final ActionType type;
    private final List<String> fields;

    Action(ActionType type, List<String> fields) {
      this.type = type;
      this.fields = fields;
    }

    static Action parse(String actionValue) {
      Matcher m = ACTION_PATTERN.matcher(actionValue);
      if (!m.matches()) {
        throw new SolrException(SERVER_ERROR, "'" + actionValue + "' not a valid action");
      }
      ActionType type;
      List<String> fields;
      if (m.group(1) != null) {
        if ("skip".equals(m.group(1))) {
          type = ActionType.SKIP;
        } else {
          type = ActionType.INSERT;
        }
        fields = null;
      } else if (m.group(2) != null) {
        if ("upsert".equals(m.group(2))) {
          type = ActionType.UPSERT;
        } else {
          type = ActionType.RETAIN;
        }
        String fieldsConfig = m.group(3);
        fields = Arrays.asList(fieldsConfig.split(","));
      } else {
        type = ActionType.NULLIFY;
        String fieldsConfig = m.group(5);
        fields = Arrays.asList(fieldsConfig.split(","));
      }
      return new Action(type, fields);
    }

    void run(SolrInputDocument oldDoc, SolrInputDocument newDoc) {
      if (type == ActionType.UPSERT || type == ActionType.RETAIN) {
        if (oldDoc == null) {
          return;
        }
        Collection<String> fieldsToCopy;
        if (ALL_FIELDS.equals(fields)) {
          fieldsToCopy = oldDoc.keySet();
        } else {
          fieldsToCopy = fields;
        }
        fieldsToCopy.forEach(field -> {
          if (type == ActionType.RETAIN || !newDoc.containsKey(field)) {
            SolrInputField inputField = oldDoc.getField(field);
            newDoc.put(field, inputField);
          }
        });
      } else if (type == ActionType.NULLIFY) {
        fields.forEach(field -> {
          newDoc.setField(field, null);
        });
      }
    }
  }
}

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.search.BooleanClause;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

class UpsertCondition {
  private static final Pattern ACTION_PATTERN = Pattern.compile("^(skip)|(upsert):(\\*|[\\w,]+)$");
  private static final List<String> ALL_FIELDS = Collections.singletonList("*");

  private final String name;
  private final List<FieldRule> rules;
  private final boolean skip;
  private final List<String> upsertFields;

  UpsertCondition(String name, boolean skip, List<String> upsertFields, List<FieldRule> rules) {
    this.name = name;
    this.skip = skip;
    this.upsertFields = upsertFields;
    this.rules = rules;
  }

  static UpsertCondition parse(String name, NamedList<String> args) {
    List<FieldRule> rules = new ArrayList<>();
    boolean skip = false;
    List<String> upsertFields = null;
    for (Map.Entry<String, String> entry: args) {
      String key = entry.getKey();
      if ("action".equals(key)) {
        String action = entry.getValue();
        Matcher m = ACTION_PATTERN.matcher(action);
        if (!m.matches()) {
          throw new SolrException(SERVER_ERROR, "'" + action + "' not a valid action");
        }
        if (m.group(1) != null) {
          skip = true;
          upsertFields = null;
        } else {
          skip = false;
          String fields = m.group(3);
          upsertFields = Arrays.asList(fields.split(","));
        }
      } else {
        BooleanClause.Occur occur = BooleanClause.Occur.valueOf(key.toUpperCase(Locale.ROOT));
        String value = entry.getValue();
        rules.add(FieldRule.parse(occur, value));
      }
    }
    if (!skip && upsertFields == null) {
      throw new SolrException(SERVER_ERROR, "no action defined for condition: " + name);
    }
    return new UpsertCondition(name, skip, upsertFields, rules);
  }

  String getName() {
    return name;
  }

  boolean isSkip() {
    return skip;
  }

  void copyOldDocFields(SolrInputDocument oldDoc, SolrInputDocument newDoc) {
    Collection<String> fieldsToCopy;
    if (ALL_FIELDS.equals(upsertFields)) {
      fieldsToCopy = oldDoc.keySet();
    } else {
      fieldsToCopy = upsertFields;
    }
    fieldsToCopy.forEach(field -> {
      if (!newDoc.containsKey(field)) {
        SolrInputField inputField = oldDoc.getField(field);
        newDoc.put(field, inputField);
      }
    });
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
          break;
        default:
          atLeastOneMatched = ruleMatched || atLeastOneMatched;
          break;
      }
    }
    return atLeastOneMatched;
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
    private static final Pattern RULE_CONDITION_PATTERN = Pattern.compile("^(OLD|NEW)\\.(\\w+):(\\w+)$");

    private final BooleanClause.Occur occur;
    private final Function<Docs, SolrInputDocument> docGetter;
    private final String field;
    private final String value;

    private FieldRule(BooleanClause.Occur occur, Function<Docs, SolrInputDocument> docGetter, String field, String value) {
      this.occur = occur;
      this.docGetter = docGetter;
      this.field = field;
      this.value = value;
    }

    static FieldRule parse(BooleanClause.Occur occur, String condition) {
      Matcher m = RULE_CONDITION_PATTERN.matcher(condition);
      if (m.matches()) {
        String doc = m.group(1);
        String field = m.group(2);
        String value = m.group(3);
        Function<Docs, SolrInputDocument> docGetter;
        if (doc.equalsIgnoreCase("OLD")) {
          docGetter = Docs::getOldDoc;
        } else {
          docGetter = Docs::getNewDoc;
        }
        return new FieldRule(occur, docGetter, field, value);
      }
      throw new SolrException(SERVER_ERROR, "'" + condition + "' not a valid condition for rule");
    }

    BooleanClause.Occur getOccur() {
      return occur;
    }

    boolean matches(Docs docs) {
      SolrInputDocument doc = docGetter.apply(docs);
      return value.equals(doc.getFieldValue(field));
    }
  }
}

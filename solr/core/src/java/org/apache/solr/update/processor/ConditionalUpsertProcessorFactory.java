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
package org.apache.solr.update.processor;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
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
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConditionalUpsertProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final List<Condition> conditions = new ArrayList<>();

  @Override
  public void init(NamedList args)  {
    for (Map.Entry<String, ?> entry: (NamedList<?>)args) {
      String name = entry.getKey();
      Object tmp = entry.getValue();
      if (tmp instanceof NamedList) {
        NamedList<String> condition = (NamedList<String>)tmp;
        conditions.add(Condition.parse(name, condition));
      }
      // TODO errors etc
    }
    super.init(args);
  }

  @Override
  public ConditionalUpsertUpdateProcessor getInstance(SolrQueryRequest req,
                                                          SolrQueryResponse rsp,
                                                          UpdateRequestProcessor next) {
    return new ConditionalUpsertUpdateProcessor(req, next, conditions);
  }

  @Override
  public void inform(SolrCore core) {
    if (core.getUpdateHandler().getUpdateLog() == null) {
      throw new SolrException(SERVER_ERROR, "updateLog must be enabled.");
    }

    if (core.getLatestSchema().getUniqueKeyField() == null) {
      throw new SolrException(SERVER_ERROR, "schema must have uniqueKey defined.");
    }
  }

  static class ConditionalUpsertUpdateProcessor extends UpdateRequestProcessor {
    private final SolrCore core;

    private DistributedUpdateProcessor distribProc;  // the distributed update processor following us
    private DistributedUpdateProcessor.DistribPhase phase;
    private final List<Condition> conditions;

    ConditionalUpsertUpdateProcessor(SolrQueryRequest req,
                                     UpdateRequestProcessor next,
                                     List<Condition> conditions) {
      super(next);
      this.core = req.getCore();
      this.conditions = conditions;

      for (UpdateRequestProcessor proc = next ;proc != null; proc = proc.next) {
        if (proc instanceof DistributedUpdateProcessor) {
          distribProc = (DistributedUpdateProcessor)proc;
          break;
        }
      }

      if (distribProc == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "DistributedUpdateProcessor must follow DocComplianceUpdateProcessor");
      }

      phase = DistributedUpdateProcessor.DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));
    }

    boolean isLeader(UpdateCommand cmd) {
      if ((cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
        return false;
      }
      if (phase == DistributedUpdateProcessor.DistribPhase.FROMLEADER) {
        return false;
      }
      return distribProc.isLeader(cmd);
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      BytesRef indexedDocId = cmd.getIndexedId();

      if (!conditions.isEmpty() && isLeader(cmd)) {
        SolrInputDocument newDoc = cmd.getSolrInputDocument();
        SolrInputDocument oldDoc = RealTimeGetComponent.getInputDocument(core, indexedDocId);
        if (oldDoc != null) {
          Docs docs = new Docs(oldDoc, newDoc);
          for (Condition condition: conditions) {
            if (condition.matches(docs)) {
              log.info("Condition {} matched, taking action", condition.getName());
              if (condition.isSkip()) {
                log.info("Condition {} matched - skipping insert", condition.getName());
                return;
              }
              condition.copyOldDocFields(docs);
              break;
            }
          }
        }
      }
      super.processAdd(cmd);
    }
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

  private static class Condition {
    private static final Pattern ACTION_PATTERN = Pattern.compile("^(skip)|(upsert):(\\*|[\\w,]+)$");
    private static final List<String> ALL_FIELDS = Collections.singletonList("*");

    private final String name;
    private final List<FieldRule> rules;
    private final boolean skip;
    private final List<String> upsertFields;

    Condition(String name, boolean skip, List<String> upsertFields, List<FieldRule> rules) {
      this.name = name;
      this.skip = skip;
      this.upsertFields = upsertFields;
      this.rules = rules;
    }

    static Condition parse(String name, NamedList<String> args) {
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
      return new Condition(name, skip, upsertFields, rules);
    }

    String getName() {
      return name;
    }

    boolean isSkip() {
      return skip;
    }

    void copyOldDocFields(Docs docs) {
      SolrInputDocument oldDoc = docs.getOldDoc();
      SolrInputDocument newDoc = docs.getNewDoc();
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

    boolean matches(Docs docs) {
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

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.plugin.SolrCoreAware;

public class ConditionalUpsertProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware, UpdateRequestProcessorFactory.RunAlways {
  private static final String UPSERT_WHEN = "upsertWhen";
  private static final String SKIP_WHEN = "skipWhen";

  private FieldCondition upsertCondition = null;
  private FieldCondition skipCondition = null;

  @Override
  public void init(NamedList args)  {
    upsertCondition = fromArgs(args, UPSERT_WHEN);
    skipCondition = fromArgs(args, SKIP_WHEN);

    super.init(args);
  }

  @Override
  public ConditionalUpsertUpdateProcessor getInstance(SolrQueryRequest req,
                                                          SolrQueryResponse rsp,
                                                          UpdateRequestProcessor next) {
    return new ConditionalUpsertUpdateProcessor(req, next, upsertCondition, skipCondition);
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
    private final FieldCondition upsertCondition;
    private final FieldCondition skipCondition;

    ConditionalUpsertUpdateProcessor(SolrQueryRequest req,
                                 UpdateRequestProcessor next,
                                 FieldCondition upsertCondition,
                                 FieldCondition skipCondition) {
      super(next);
      this.core = req.getCore();
      this.upsertCondition = upsertCondition;
      this.skipCondition = skipCondition;

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

      if ((upsertCondition != null || skipCondition != null) && isLeader(cmd)) {
        SolrInputDocument newDoc = cmd.getSolrInputDocument();
        SolrInputDocument oldDoc = RealTimeGetComponent.getInputDocument(core, indexedDocId);
        if (oldDoc != null) {
          if (skipCondition != null && skipCondition.matches(oldDoc)) {
            // don't add the doc (tombstone present)
            return;
          }
          if (upsertCondition != null) {
            if (upsertCondition.matches(newDoc)) {
              // ensure we copy over the other old fields
              oldDoc.forEach((field, value) -> {
                if (!newDoc.containsKey(field)) {
                  newDoc.put(field, value);
                }
              });
            } else if (upsertCondition.matches(oldDoc)) {
              // ensure we copy over the old value for the upsert field
              String upsertField = upsertCondition.getField();
              Object existingValue = oldDoc.getFieldValue(upsertField);
              newDoc.setField(upsertField, existingValue);
            }
          }
        }
      }
      super.processAdd(cmd);
    }
  }

  FieldCondition fromArgs(NamedList args, String name) {
    Object tmp = args.remove(name);
    if (null != tmp) {
      if (! (tmp instanceof String) ) {
        throw new SolrException(SERVER_ERROR, "'" + name + "' must be configured as a <string>");
      }
      return parse((String)tmp);
    }
    return null;
  }

  FieldCondition parse(String condition) {
    Pattern p = Pattern.compile("^(\\w+):(\\w+)$");
    Matcher m = p.matcher(condition);
    if (m.matches()) {
      String field = m.group(1);
      String value = m.group(2);
      return new FieldCondition(field, value);
    }
    throw new SolrException(SERVER_ERROR, "'" + condition + "' not a valid condition");
  }

  static class FieldCondition {
    private final String field;
    private final String value;

    FieldCondition(String field, String value) {
      this.field = field;
      this.value = value;
    }

    String getField() {
      return field;
    }

    boolean matches(SolrInputDocument doc) {
      return value.equals(doc.getFieldValue(field));
    }
  }
}

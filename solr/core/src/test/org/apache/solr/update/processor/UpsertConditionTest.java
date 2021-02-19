package org.apache.solr.update.processor;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

public class UpsertConditionTest {

  @Test(expected = SolrException.class)
  public void givenNoAction_whenParsingCondition() {
    NamedList<String> args = new NamedList<>();
    args.add("must", "OLD.field:value");
    UpsertCondition.parse("no-action", args);
  }

  @Test(expected = SolrException.class)
  public void givenInvalidMatchOccurrence_whenParsingCondition() {
    NamedList<String> args = new NamedList<>();
    args.add("maybe_might", "OLD.field:value");
    args.add("action", "skip");
    UpsertCondition.parse("bad-occurrence", args);
  }

  @Test(expected = SolrException.class)
  public void givenNoRules_whenParsingCondition() {
    NamedList<String> args = new NamedList<>();
    args.add("action", "skip");
    UpsertCondition.parse("no-rules", args);
  }

  @Test(expected = SolrException.class)
  public void givenBadRuleDocPart_whenParsingCondition() {
    NamedList<String> args = new NamedList<>();
    args.add("must", "YOUNG.field:value");
    args.add("action", "skip");
    UpsertCondition.parse("bad-rule", args);
  }

  @Test(expected = SolrException.class)
  public void givenNoDocPart_whenParsingCondition() {
    NamedList<String> args = new NamedList<>();
    args.add("must", "field:value");
    args.add("action", "skip");
    UpsertCondition.parse("bad-rule", args);
  }

  @Test(expected = SolrException.class)
  public void givenNoValuePart_whenParsingCondition() {
    NamedList<String> args = new NamedList<>();
    args.add("must", "OLD.field");
    args.add("action", "skip");
    UpsertCondition.parse("bad-rule", args);
  }

  @Test(expected = SolrException.class)
  public void givenBadAction_whenParsingCondition() {
    NamedList<String> args = new NamedList<>();
    args.add("must", "OLD.field:value");
    args.add("action", "skippy");
    UpsertCondition.parse("bad-action", args);
  }

  @Test
  public void givenSingleMustClause_whenMatching() {
    NamedList<String> args = new NamedList<>();
    args.add("must", "OLD.field:value");
    args.add("action", "skip");

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.isSkip(), is(true));
    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "value");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "not-value");
    assertFalse(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenSingleShouldClause_whenMatching() {
    NamedList<String> args = new NamedList<>();
    args.add("should", "OLD.field:value");
    args.add("action", "skip");

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.isSkip(), is(true));
    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "value");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "not-value");
    assertFalse(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenSingleMustNotClause_whenMatching() {
    NamedList<String> args = new NamedList<>();
    args.add("must_not", "OLD.field:value");
    args.add("action", "skip");

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.isSkip(), is(true));
    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "value");
    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field", "not-value");
    assertTrue(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenMultipleMustClauses_whenMatching() {
    NamedList<String> args = new NamedList<>();
    args.add("must", "OLD.field1:value1");
    args.add("must", "NEW.field2:value2");
    args.add("action", "skip");

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.isSkip(), is(true));
    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    newDoc.setField("field2", "value2");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "not-value1");
    newDoc.setField("field2", "value2");
    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.removeField("field1");
    assertFalse(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenMultipleShouldClauses_whenMatching() {
    NamedList<String> args = new NamedList<>();
    args.add("should", "OLD.field1:value1");
    args.add("should", "NEW.field2:value2");
    args.add("action", "skip");

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.isSkip(), is(true));
    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    newDoc.setField("field2", "value2");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "not-value1");
    newDoc.setField("field2", "value2");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.removeField("field1");
    assertTrue(condition.matches(oldDoc, newDoc));

    newDoc.setField("field2", "not-value2");
    assertFalse(condition.matches(oldDoc, newDoc));
  }

  @Test
  public void givenMustAndMustNotClauses_whenMatching() {
    NamedList<String> args = new NamedList<>();
    args.add("must", "OLD.field1:value1");
    args.add("must_not", "NEW.field2:value2");
    args.add("action", "skip");

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.isSkip(), is(true));
    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    assertTrue(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    newDoc.setField("field2", "value2");
    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "not-value1");
    newDoc.setField("field2", "value2");
    assertFalse(condition.matches(oldDoc, newDoc));

    oldDoc.setField("field1", "value1");
    newDoc.setField("field2", "not-value2");
    assertTrue(condition.matches(oldDoc, newDoc));
  }

  @Test(expected = IllegalStateException.class)
  public void givenSkipAction_whenCopyingOldFields() {
    NamedList<String> args = new NamedList<>();
    args.add("should", "OLD.field:value");
    args.add("action", "skip");

    UpsertCondition condition = UpsertCondition.parse("skip-it", args);

    assertThat(condition.isSkip(), is(true));
    assertThat(condition.getName(), is("skip-it"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();

    condition.copyOldDocFields(oldDoc, newDoc);
  }

  @Test
  public void givenUpsertForSpecificFields_whenCopyingOldFields() {
    NamedList<String> args = new NamedList<>();
    args.add("must", "OLD.field:value");
    args.add("action", "upsert:field,other_field");

    UpsertCondition condition = UpsertCondition.parse("upsert", args);

    assertThat(condition.isSkip(), is(false));
    assertThat(condition.getName(), is("upsert"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();
    oldDoc.setField("field", "value");
    oldDoc.setField("other_field", "old-value");
    oldDoc.setField("not-copied", "not-copied");

    condition.copyOldDocFields(oldDoc, newDoc);

    assertThat(newDoc.getFieldValue("field"), is("value"));
    assertThat(newDoc.getFieldValue("other_field"), is("old-value"));
    assertFalse(newDoc.containsKey("not-copied"));

    newDoc = new SolrInputDocument();
    newDoc.setField("field", "left-alone");

    condition.copyOldDocFields(oldDoc, newDoc);

    assertThat(newDoc.getFieldValue("field"), is("left-alone"));
    assertThat(newDoc.getFieldValue("other_field"), is("old-value"));
    assertFalse(newDoc.containsKey("not-copied"));
  }

  @Test
  public void givenUpsertForAllFields_whenCopyingOldFields() {
    NamedList<String> args = new NamedList<>();
    args.add("must", "OLD.field:value");
    args.add("action", "upsert:*");

    UpsertCondition condition = UpsertCondition.parse("upsert", args);

    assertThat(condition.isSkip(), is(false));
    assertThat(condition.getName(), is("upsert"));

    SolrInputDocument oldDoc = new SolrInputDocument();
    SolrInputDocument newDoc = new SolrInputDocument();
    oldDoc.setField("field", "value");
    oldDoc.setField("other_field", "old-value");
    oldDoc.setField("also-copied", "also-copied");

    condition.copyOldDocFields(oldDoc, newDoc);

    assertThat(newDoc.getFieldValue("field"), is("value"));
    assertThat(newDoc.getFieldValue("other_field"), is("old-value"));
    assertThat(newDoc.getFieldValue("also-copied"), is("also-copied"));

    newDoc = new SolrInputDocument();
    newDoc.setField("field", "left-alone");

    condition.copyOldDocFields(oldDoc, newDoc);

    assertThat(newDoc.getFieldValue("field"), is("left-alone"));
    assertThat(newDoc.getFieldValue("other_field"), is("old-value"));
    assertThat(newDoc.getFieldValue("also-copied"), is("also-copied"));
  }
}

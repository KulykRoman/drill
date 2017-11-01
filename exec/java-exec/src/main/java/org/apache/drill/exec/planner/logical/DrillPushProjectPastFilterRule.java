/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.logical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;

public class DrillPushProjectPastFilterRule extends ProjectFilterTransposeRule {

  private final PushProjector.ExprCondition preserveExprCondition;
  public final static RelOptRule INSTANCE = new DrillPushProjectPastFilterRule(DrillConditions.PRESERVE_ITEM);

  //Changes to support Calcite 1.13.
  protected DrillPushProjectPastFilterRule(PushProjector.ExprCondition preserveExprCondition) {
    super(LogicalProject.class, LogicalFilter.class, RelFactories.LOGICAL_BUILDER, preserveExprCondition);
    this.preserveExprCondition = preserveExprCondition;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Project origProj;
    Filter filter;
    if (call.rels.length >= 2) {
      origProj = call.rel(0);
      filter = call.rel(1);
    } else {
      origProj = null;
      filter = call.rel(0);
    }
    RelNode rel = filter.getInput();
    RexNode origFilter = filter.getCondition();

    if ((origProj != null)
        && RexOver.containsOver(origProj.getProjects(), null)) {
      return;
    }

    PushProjector pushProjector =
        new PushProjector(
            origProj, origFilter, rel, preserveExprCondition, call.builder());
    RelNode topProject;
    if (!pushProjector.locateAllRefs() && origProj != null) {
      topProject = convertProject(pushProjector, rel, origFilter);
    } else {
      //RelNode topProject = convertProject(null, pushProjector, rel, origFilter, origProj);
      topProject = pushProjector.convertProject(null);
    }
    if (topProject != null) {
      call.transformTo(topProject);
    }
  }

  public RelNode convertProject(PushProjector pushProjector, RelNode rel, RexNode origFilter) {
    RelNode newProject = pushProjector.createProjectRefsAndExprs(rel, false, false);
    int[] adjustments = pushProjector.getAdjustments();
    RelNode projChild;
    if (origFilter != null) {
      RexNode newFilter =
          pushProjector.convertRefsAndExprs(
              origFilter,
              newProject.getRowType().getFieldList(),
              adjustments);
      projChild = RelOptUtil.createFilter(newProject, newFilter);
    } else {
      projChild = newProject;
    }
    return pushProjector.createNewProject(projChild, adjustments);
  }
}

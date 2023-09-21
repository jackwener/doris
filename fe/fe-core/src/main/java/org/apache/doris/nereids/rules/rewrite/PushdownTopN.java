// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Same with PushdownLimit
 */
public class PushdownTopN implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // topN -> join
                logicalTopN(logicalJoin())
                        .then(topN -> {
                            long limitNum = topN.getLimit();
                            long offsetNum = topN.getOffset();
                            LogicalJoin<Plan, Plan> join = topN.child();

                            Plan newJoin = PushdownLimit.pushLimitThroughJoin(limitNum, offsetNum, join);
                            if (newJoin == null || topN.child().children().equals(newJoin.children())) {
                                return null;
                            }
                            return topN.withChildren(newJoin);
                        })
                        .toRule(RuleType.PUSH_TOP_N_THROUGH_JOIN),

                // topN -> project -> join
                logicalTopN(logicalProject(logicalJoin()))
                        .then(topN -> {
                            long limitNum = topN.getLimit();
                            long offsetNum = topN.getOffset();
                            LogicalProject<LogicalJoin<Plan, Plan>> project = topN.child();
                            LogicalJoin<Plan, Plan> join = project.child();

                            Plan newJoin = PushdownLimit.pushLimitThroughJoin(limitNum, offsetNum, join);
                            if (newJoin == null || join.children().equals(newJoin.children())) {
                                return null;
                            }
                            return topN.withChildren(project.withChildren(newJoin));
                        }).toRule(RuleType.PUSH_TOP_N_THROUGH_PROJECT_JOIN),

                // limit -> union
                logicalTopN(logicalUnion(multi()).when(union -> union.getQualifier() == Qualifier.ALL))
                        .then(topN -> {
                            long limitNum = topN.getLimit();
                            long offsetNum = topN.getOffset();
                            LogicalUnion union = topN.child();
                            ImmutableList<Plan> newUnionChildren = union.children()
                                    .stream()
                                    .map(child -> new LogicalLimit<>(limitNum, offsetNum, LimitPhase.ORIGIN, child))
                                    .collect(ImmutableList.toImmutableList());
                            if (union.children().equals(newUnionChildren)) {
                                return null;
                            }
                            return topN.withChildren(union.withChildren(newUnionChildren));
                        })
                        .toRule(RuleType.PUSH_TOP_N_THROUGH_UNION),
                new MergeLimits().build()
        );
    }

}

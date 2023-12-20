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

//! Tree node implementation for logical plan

use crate::LogicalPlan;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, VisitRecursionIterator};
use datafusion_common::Result;

impl TreeNode for LogicalPlan {
    fn visit_children<F>(&self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>,
    {
        self.inputs().into_iter().for_each_till_continue(f)
    }

    fn visit_inner_children<F>(&self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>,
    {
        self.visit_subqueries(f)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let old_children = self.inputs();
        let new_children = old_children
            .iter()
            .map(|&c| c.clone())
            .map(transform)
            .collect::<Result<Vec<_>>>()?;

        // if any changes made, make a new child
        if old_children
            .iter()
            .zip(new_children.iter())
            .any(|(c1, c2)| c1 != &c2)
        {
            self.with_new_inputs(new_children.as_slice())
        } else {
            Ok(self)
        }
    }

    fn transform_children<F>(&mut self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&mut Self) -> Result<TreeNodeRecursion>,
    {
        let old_children = self.inputs();
        let mut new_children =
            old_children.iter().map(|&c| c.clone()).collect::<Vec<_>>();
        let tnr = new_children.iter_mut().for_each_till_continue(f)?;

        // if any changes made, make a new child
        if old_children
            .iter()
            .zip(new_children.iter())
            .any(|(c1, c2)| c1 != &c2)
        {
            *self = self.with_new_inputs(new_children.as_slice())?;
        }
        Ok(tnr)
    }
}

#[cfg(test)]
mod test {
    use crate::{LogicalPlan, LogicalPlanBuilder};
    use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
    use std::time::Instant;

    fn create_union_tree(level: u32) -> LogicalPlanBuilder {
        if level == 0 {
            LogicalPlanBuilder::empty(true)
        } else {
            create_union_tree(level - 1)
                .union(create_union_tree(level - 1).build().unwrap())
                .unwrap()
        }
    }

    #[test]
    fn transform_test() {
        let now = Instant::now();
        let mut union_tree = create_union_tree(23).build().unwrap();
        println!("create_union_tree: {}", now.elapsed().as_millis());

        let now = Instant::now();
        union_tree = union_tree
            .transform_down_old(&mut |p| Ok(Transformed::No(p)))
            .unwrap();
        println!(
            "union_tree.transform_down_old: {}",
            now.elapsed().as_millis()
        );

        let now = Instant::now();
        let mut union_tree_clone = union_tree.clone();
        println!("union_tree.clone: {}", now.elapsed().as_millis());

        let now = Instant::now();
        union_tree_clone
            .transform_down(&mut |_p| Ok(TreeNodeRecursion::Continue))
            .unwrap();
        println!(
            "union_tree_clone.transform_down: {}",
            now.elapsed().as_millis()
        );

        println!("results: {}", union_tree == union_tree_clone);

        let now = Instant::now();
        union_tree = union_tree
            .transform_down_old(&mut |p| match p {
                LogicalPlan::EmptyRelation(_) => Ok(Transformed::Yes(
                    LogicalPlanBuilder::empty(false).build().unwrap(),
                )),
                o => Ok(Transformed::No(o)),
            })
            .unwrap();
        println!(
            "union_tree.transform_down_old 2: {}",
            now.elapsed().as_millis()
        );

        // Need to keep a reference to `union_tree_clone`  till the end so as not to account its destruction to any experiment
        let union_tree_clone2 = union_tree_clone.clone();

        let now = Instant::now();
        union_tree_clone
            .transform_down(&mut |p| match p {
                LogicalPlan::EmptyRelation(_) => {
                    *p = LogicalPlanBuilder::empty(false).build().unwrap();
                    Ok(TreeNodeRecursion::Continue)
                }
                _ => Ok(TreeNodeRecursion::Continue),
            })
            .unwrap();
        println!(
            "union_tree_clone.transform_down 2: {}",
            now.elapsed().as_millis()
        );

        println!("results: {}", union_tree == union_tree_clone);

        println!("results: {}", union_tree_clone == union_tree_clone2);
    }
}

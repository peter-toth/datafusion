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

extern crate arrow;
#[macro_use]
extern crate criterion;

use crate::criterion::Criterion;
use datafusion_common::tree_node::tests::test_dyn_tree_node::{
    tall_tree, visit_tree_new, visit_tree_old, wide_tree,
};

fn criterion_benchmark(c: &mut Criterion) {
    let tall_tree = tall_tree();
    let wide_tree = wide_tree();

    c.bench_function("visit old tall tree", |b| {
        b.iter(|| visit_tree_old(tall_tree.clone()))
    });
    c.bench_function("visit new tall tree", |b| {
        b.iter(|| visit_tree_new(tall_tree.clone()))
    });

    c.bench_function("visit old wide tree", |b| {
        b.iter(|| visit_tree_old(wide_tree.clone()))
    });
    c.bench_function("visit new wide tree", |b| {
        b.iter(|| visit_tree_new(wide_tree.clone()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Debug, Formatter};
use std::time::Duration;

pub struct Metrics {
    pub output_size: usize,
    pub elapsed_time: Duration,
}

impl Debug for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let size = (self.output_size * 100 / 1024 / 1024) as f64  / 100f64;
        let time = self.elapsed_time.as_millis() as f64 / 1000f64;
        f.debug_struct("Metrics").field("output_size", &size)
            .field("elapsed_time", &time).finish()
    }
}

//! Memory metrics.

use std::fmt;

use common_base::readable_size::ReadableSize;
use tikv_jemalloc_ctl::{epoch, stats};

/// Memory metrics.
pub struct MemoryMetrics {
    /// Allocated bytes.
    pub allocated: usize,
    /// Resident bytes.
    pub resident: usize,
}

impl fmt::Debug for MemoryMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryMetrics")
            .field("allocated", &DisplayBytes(self.allocated))
            .field("resident", &DisplayBytes(self.resident))
            .finish()
    }
}

impl MemoryMetrics {
    /// Read metrics from jemalloc.
    pub fn read_metrics() -> MemoryMetrics {
        // many statistics are cached and only updated when the epoch is advanced.
        epoch::advance().unwrap();

        let allocated = stats::allocated::read().unwrap();
        let resident = stats::resident::read().unwrap();

        MemoryMetrics {
            allocated,
            resident,
        }
    }

    /// Subtract memory allocated.
    ///
    /// If the allocated memory is less than `before`, return 0.
    pub fn subtract_allocated(&self, before: &MemoryMetrics) -> usize {
        if self.allocated > before.allocated {
            self.allocated - before.allocated
        } else {
            0
        }
    }
}

/// Helper to debug print bytes in readable format.
pub(crate) struct DisplayBytes(pub(crate) usize);

impl fmt::Debug for DisplayBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ReadableSize(self.0 as u64))
    }
}

mod disk_manager;

// Reexport so we can refer it from other mod
// as crate::storage::DiskManager instead of
// crate::storage::disk_manager::DiskManager
pub use self::disk_manager::DiskManager;

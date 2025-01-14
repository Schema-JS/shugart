
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::SystemTime;
use memmap2::MmapMut;
use tokio::fs::{File, OpenOptions};
use uuid::Uuid;
use crate::cursor::Cursor;
use crate::disk_metadata::{DiskMetadata, DiskMetadataV1};
use crate::{DiskError, U64_SIZE};
use crate::utils::get_created_at;

#[derive(Clone)]
pub struct DiskConf<P: AsRef<Path> + Clone> {
    pub capacity: u64,
    pub max_items: u64,
    pub disk_file_path: P
}

pub struct Disk {
    pub id: Uuid,
    pub capacity: u64,
    pub max_items: u64,
    pub path: PathBuf,
    mmap: MmapMut,
    write_offset: AtomicUsize,
    locked: AtomicBool,
    pub busy: AtomicUsize, // Tracks the number of active writes,
    metadata: DiskMetadata,
    file: File,
    metadata_size: u64
}

/// Initialized flag + Locked flag + Metadata Length
pub const COMMIT_LOG_INITIAL_HEADER_SIZE: usize = 1 + 1 + 8;


/// | Byte Range | Description                | Details                      |
/// |------------|----------------------------|------------------------------|
/// | 0          | Initialized flag (1 byte) | 0 = uninitialized, 1 = initialized |
/// | 1          | Locked flag (1 byte)      | 0 = Unlocked, 1 = Locked |
/// | 2-10       | Metadata Length (8 bytes) | Legnth of associated metadata in bytes |
/// | 10...      | Metadata payload (variable) | The actual metadata payload |
impl Disk {
    pub async fn new<P: AsRef<Path> + Clone>(opts: DiskConf<P>) -> Self {
        let DiskConf { disk_file_path, capacity, max_items } = opts;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&disk_file_path)
            .await
            .unwrap();

        file.set_len(capacity).await.unwrap();

        // Memory-map the file
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };

        let (locked, metadata, metadata_size) = Self::read_metadata(&mut mmap);

        let write_offset_begin_at = COMMIT_LOG_INITIAL_HEADER_SIZE + metadata_size;

        Self {
            id: Uuid::new_v4(),
            mmap,
            write_offset: AtomicUsize::new(write_offset_begin_at), // It starts from 2 because [initialized, locked]
            capacity,
            locked: AtomicBool::from(locked),
            busy: AtomicUsize::new(0),
            path: disk_file_path.as_ref().to_path_buf(),
            max_items,
            metadata,
            file,
            metadata_size: metadata_size as u64
        }
    }

    fn curr_writing_offset(&self) -> usize {
        self.write_offset.load(Ordering::Relaxed)
    }

    /// Set the lock state (true for locked, false for unlocked)
    fn set_locked(&self, locked: bool) -> Result<(), DiskError> {
        // Update the in-memory AtomicBool
        self.locked.store(locked, Ordering::Release);

        // Update the mmap to reflect the lock state
        let lock_flag = if locked { 1u8 } else { 0u8 };

        // Unsafe write to mmap with synchronization
        unsafe {
            let lock_ptr = self.mmap.as_ptr().add(1) as *mut u8;
            std::ptr::write(lock_ptr, lock_flag);
        }

        // Flush the mmap to persist changes
        self.mmap
            .flush()
            .map_err(|_| DiskError::InvalidFlushing)
    }


    pub fn reserve_space(&self, size: usize) -> Result<usize, DiskError> {
        // Check if the log is locked before proceeding
        if self.is_locked() {
            return Err(DiskError::Locked);
        }

        // Atomically reserve space
        let offset = self.write_offset.fetch_add(size, Ordering::SeqCst);

        if offset + size > self.capacity as usize {
            Err(DiskError::CapacityReached)
        } else {
            Ok(offset)
        }
    }

    fn initialize_file(mmap: &mut MmapMut) {
        mmap[0] = 1u8; // Mark as initialized
        mmap[1] = 0u8; // Mark as unlocked
        mmap.flush().expect("Failed to flush mmap during initialization");
    }

    fn read_metadata(mmap: &mut MmapMut) -> (bool, DiskMetadata, usize) {
        let mut cursor = Cursor::mmap_mut(mmap);

        // Read the first two bytes to determine initialization and lock status
        let initialized_locked_val = cursor.consume(2).expect("Failed to read initialization and lock bytes");
        let initialized = initialized_locked_val[0] == 1u8;
        let locked = initialized_locked_val[1] == 1u8;

        let (metadata, metadata_size) = if initialized {
            Self::read_existing_metadata(&mut cursor)
        } else {
            Self::initialize_file(mmap);
            Self::create_and_store_metadata(mmap)
        };

        (locked, metadata, metadata_size)
    }

    fn create_and_store_metadata(mmap: &mut MmapMut) -> (DiskMetadata, usize) {
        let metadata = DiskMetadata::V1(DiskMetadataV1 {
            created_at: get_created_at(SystemTime::now())
        });

        let metadata_bytes = metadata.to_vec();
        let metadata_length = metadata_bytes.len();

        // Store metadata size and metadata itself
        mmap[2..10].copy_from_slice(&metadata_length.to_le_bytes());
        mmap[10..(10 + metadata_length)].copy_from_slice(&metadata_bytes);

        mmap.flush().expect("Failed to flush mmap during metadata creation");

        (metadata, metadata_length)
    }

    fn read_existing_metadata(cursor: &mut Cursor) -> (DiskMetadata, usize) {
        // Read metadata size
        let metadata_size_bytes = cursor.consume(U64_SIZE).expect("Failed to read metadata size");
        let metadata_size = u64::from_le_bytes(metadata_size_bytes.try_into().expect("Invalid metadata size bytes"));

        // Read metadata
        let metadata_bytes = cursor
            .consume(metadata_size as usize)
            .expect("Failed to read metadata bytes");

        (DiskMetadata::try_from(metadata_bytes.to_vec()).unwrap(), metadata_size as usize)
    }

    /// Check if the log is locked
    fn is_locked(&self) -> bool {
        self.locked.load(Ordering::Acquire)
    }

    fn write(&self, data: &[u8], start_at: usize) -> Result<(), DiskError> {
        // Check if the log is locked before proceeding
        if self.is_locked() {
            return Err(DiskError::Locked);
        }

        // Indicate the log is busy by incrementing the counter
        self.busy.fetch_add(1, Ordering::SeqCst);

        let len = data.len();

        if self.is_locked() {
            self.busy.fetch_sub(1, Ordering::SeqCst); // Decrement on failure
            return Err(DiskError::Locked);
        }

        unsafe {
            // Access the mmap memory as a raw pointer
            let mmap_ptr = self.mmap.as_ptr() as *mut u8;

            // Write data into the reserved region using raw pointer arithmetic
            std::ptr::copy_nonoverlapping(data.as_ptr(), mmap_ptr.add(start_at), len);
        }

        self.busy.fetch_sub(1, Ordering::SeqCst);

        Ok(())
    }

    pub fn flush(&self) -> Result<(), DiskError> {
        // Mark the log as no longer busy
        self.busy.fetch_sub(1, Ordering::SeqCst);
        self.mmap.flush().map_err(|_| DiskError::InvalidFlushing)
    }
}

#[cfg(test)]
mod disk_tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;
    use tokio::time::sleep;
    use crate::disk::{Disk, DiskConf};
    use crate::DiskError;
    use crate::utils::test_utils::get_file;

    #[tokio::test]
    pub async fn test_disk_creation() {
        let fake_partial_folder_path = get_file(None, true);

        let conf = DiskConf {
            capacity: 1024,
            max_items: 1,
            disk_file_path: fake_partial_folder_path.clone(),
        };

        let disk = Disk::new(conf.clone()).await;
        assert_eq!(disk.locked.load(Ordering::Acquire), false);
        // COMMIT_LOG_INITIAL_HEADER_SIZE + 9 (9 = metadata size)
        assert_eq!(disk.write_offset.load(Ordering::Acquire), 19);
        assert!(disk.metadata.is_v1());
        sleep(Duration::from_secs(2)).await;
        let disk_2 = Disk::new(conf).await;
        assert_eq!(disk_2.metadata.as_v1().unwrap().created_at, disk.metadata.as_v1().unwrap().created_at);
    }

    #[tokio::test]
    pub async fn test_concurrency_commit_log() {
        let log = get_disk(None).await;
        let log = Arc::new(log);
        let handles: Vec<_> = (0..100)
            .map(|i| {
                let log = log.clone();
                thread::spawn(move || {
                    let entry = format!("{}", i);
                    let data = entry.as_bytes();
                    let offset = log.reserve_space(data.len()).unwrap();
                    log.write(&data, offset).unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        log.flush().unwrap();
        println!("All threads have finished writing.");
        let log = Disk::new(DiskConf {
            capacity: log.capacity,
            max_items: log.max_items,
            disk_file_path: log.path.clone(),
        }).await;

        // let mut cursor = log.get_cursor();
        // let iter = CommitLogIterator::new(&mut cursor);
        // let mut items: Vec<String> = iter
        //     .map(|e| {
        //         String::from_utf8(e.as_valid().unwrap().data.as_raw().unwrap().to_owned()).unwrap()
        //     })
        //     .collect();
        // items.sort();
        // assert_eq!(items.len(), 100);
        // assert_eq!(items[0], "0");
        // assert_eq!(items[99], "99");
        //
        // let _ = std::fs::remove_file(fake_partial_folder_path);
    }

    #[tokio::test]
    async fn test_basic_locking_behavior() {
        let log = get_disk(None).await;

        // Write to the log while unlocked
        let entry = vec![1, 2, 3, 4];
        let entry_offset = log.reserve_space(entry.len()).unwrap();
        let write = log.write(&entry, entry_offset);
        assert!(write.is_ok());

        // Lock the log
        log.set_locked(true).unwrap();

        // Attempt to write while locked
        let entry = vec![5, 6, 7, 8];
        let entry_space = log.reserve_space(entry.len());
        assert!(matches!(
            entry_space,
            Err(DiskError::Locked)
        ));

        // Unlock the log
        log.set_locked(false).unwrap();

        // Write to the log after unlocking
        let entry_data = vec![9, 10, 11, 12];
        let entry_space = log.reserve_space(entry_data.len()).unwrap();
        assert!(log.write(&entry, entry_space).is_ok());
    }

    async fn get_disk(capacity: Option<u64>) -> Disk {
        let fake_partial_folder_path = get_file(None, true);

        let conf = DiskConf {
            capacity: capacity.unwrap_or(1024),
            max_items: 1,
            disk_file_path: fake_partial_folder_path.clone(),
        };

        Disk::new(conf).await
    }

    #[tokio::test]
    async fn test_concurrent_writes_respect_lock() {
        use std::sync::{Arc, Barrier};
        use std::thread;

       let disk = get_disk(None).await;

        let disk = Arc::new(disk);
        let barrier = Arc::new(Barrier::new(3)); // 3 threads (main + 2 writers)

        let disk_arc_clone1 = Arc::clone(&disk);
        let barrier_clone1 = Arc::clone(&barrier);

        let handle1 = thread::spawn(move || {
            let data: Vec<u8> = vec![1, 2, 3, 4];
            barrier_clone1.wait(); // Synchronize start
            let reserve_space_offset = disk_arc_clone1.reserve_space(data.len())?;
            disk_arc_clone1.write(&data, reserve_space_offset)
        });

        let disk_arc_clone2 = Arc::clone(&disk);
        let barrier_clone2 = Arc::clone(&barrier);

        let handle2 = thread::spawn(move || {
            let data: Vec<u8> = vec![5, 6, 7, 8];
            barrier_clone2.wait(); // Synchronize start
            let reserve_space_offset = disk_arc_clone2.reserve_space(data.len())?;
            disk_arc_clone2.write(&data, reserve_space_offset)
        });

        // Lock the log before the threads start writing
        disk.set_locked(true);
        barrier.wait(); // Let threads proceed

        let result1 = handle1.join().unwrap();
        let result2 = handle2.join().unwrap();

        assert_eq!(result1, Err(DiskError::Locked));
        assert!(matches!(result2, Err(DiskError::Locked)));

        // Unlock the log and retry
        disk.set_locked(false);
        let entry = vec![9, 10, 11, 12];
        let reserve_space = disk.reserve_space(entry.len()).unwrap();
        assert!(disk.write(&entry, reserve_space).is_ok());
    }

    #[tokio::test]
    async fn test_commit_log_concurrent_write_with_space_limit() {
        use std::sync::{Arc, Barrier};
        use std::thread;


        let disk = get_disk(Some(28)).await;

        // Create a commit log with a small size to simulate running out of space
        let commit_log = Arc::new(disk); // Only 16 bytes available
        let barrier = Arc::new(Barrier::new(3)); // 3 threads (main + 2 writers)

        // Thread 1: Attempt to write 4 bytes
        let commit_log_clone1 = Arc::clone(&commit_log);
        let barrier_clone1 = Arc::clone(&barrier);
        let handle1 = thread::spawn(move || {
            let bytes = vec![1, 2, 3, 4];
            let space = commit_log_clone1.reserve_space(bytes.len());
            barrier_clone1.wait(); // Synchronize start
            commit_log_clone1.write(&bytes, space?)
        });

        // Thread 2: Attempt to write 8 bytes
        let commit_log_clone2 = Arc::clone(&commit_log);
        let barrier_clone2 = Arc::clone(&barrier);
        let handle2 = thread::spawn(move || {
            let bytes = vec![5, 6, 7, 8, 9, 10, 11, 12];
            let space = commit_log_clone2.reserve_space(bytes.len());
            barrier_clone2.wait(); // Synchronize start
            commit_log_clone2.write(&bytes, space?)
        });

        // Main thread waits for all threads to start
        barrier.wait();

        // Collect results from threads
        let result1 = handle1.join().expect("Thread 1 panicked");
        let result2 = handle2.join().expect("Thread 2 panicked");

        // Verify one succeeded and one failed due to space limitations
        let success_count = [&result1, &result2]
            .iter()
            .filter(|result| result.is_ok())
            .count();

        let failure_count = [&result1, &result2]
            .iter()
            .filter(|result| matches!(result, Err(DiskError::CapacityReached)))
            .count();

        assert_eq!(success_count, 1, "Exactly one thread should succeed");
        assert_eq!(
            failure_count, 1,
            "Exactly one thread should fail due to out of space"
        );
    }

    #[tokio::test]
    pub async fn test_busy_with_multiple_threads() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let commit_log = Arc::new(get_disk(None).await);
        let barrier = Arc::new(Barrier::new(3)); // 3 threads (main + 2 writers)

        // Thread 1: Increment busy counter
        let commit_log_clone1 = Arc::clone(&commit_log);
        let barrier_clone1 = Arc::clone(&barrier);
        let handle1 = thread::spawn(move || {
            barrier_clone1.wait(); // Synchronize start
            commit_log_clone1.busy.fetch_add(1, Ordering::SeqCst);
            // Simulate some work
            std::thread::sleep(std::time::Duration::from_millis(100));
            commit_log_clone1.busy.fetch_sub(1, Ordering::SeqCst);
        });

        // Thread 2: Increment busy counter
        let commit_log_clone2 = Arc::clone(&commit_log);
        let barrier_clone2 = Arc::clone(&barrier);
        let handle2 = thread::spawn(move || {
            barrier_clone2.wait(); // Synchronize start
            commit_log_clone2.busy.fetch_add(1, Ordering::SeqCst);
            // Simulate some work
            std::thread::sleep(std::time::Duration::from_millis(100));
            commit_log_clone2.busy.fetch_sub(1, Ordering::SeqCst);
        });

        barrier.wait(); // Allow threads to start writing

        // Both threads should increment busy, so the counter should be 2
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(commit_log.busy.load(Ordering::Acquire), 2);

        // After threads complete, the counter should return to 0
        handle1.join().unwrap();
        handle2.join().unwrap();
        assert_eq!(commit_log.busy.load(Ordering::Acquire), 0);
    }



}
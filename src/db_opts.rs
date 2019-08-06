use crate::tikv_code::cfg::ReadableDuration;
use crate::tikv_code::cfg::ReadableSize;
use crate::tikv_code::cfg::{GB, KB, MB};
use crate::tikv_code::constexpr::CF_DEFAULT;

use rocksdb::BlockBasedOptions;
use rocksdb::Cache;
use rocksdb::CompactionPriority;
use rocksdb::DBCompactionStyle;
use rocksdb::DBCompressionType;
use rocksdb::DBRecoveryMode;
use rocksdb::LRUCacheOptions;
use rocksdb::{self, ColumnFamilyOptions, DBOptions};

const RAFT_MIN_MEM: usize = 256 * MB as usize;
const RAFT_MAX_MEM: usize = 2 * GB as usize;

fn memory_mb_for_cf(is_raft_db: bool, cf: &str) -> usize {
    let total_mem = sys_info::mem_info().unwrap().total * KB;
    let (ratio, min, max) = match (is_raft_db, cf) {
        (true, CF_DEFAULT) => (0.02, RAFT_MIN_MEM, RAFT_MAX_MEM),
        (false, CF_DEFAULT) => (0.25, 0, std::usize::MAX),
        _ => unreachable!(),
    };
    let mut size = (total_mem as f64 * ratio) as usize;
    if size < min {
        size = min;
    } else if size > max {
        size = max;
    }
    size / MB as usize
}

/// Like db-config in line 708-797
/// The config is copy from line 671
pub fn default_db_options() -> DBOptions {
    let mut opts = DBOptions::new();

    opts.set_wal_recovery_mode(DBRecoveryMode::PointInTime);
    //  default is empty
    //    if !self.wal_dir.is_empty() {
    //        opts.set_wal_dir(&self.wal_dir);
    //    }
    opts.set_wal_ttl_seconds(0);

    opts.set_wal_size_limit_mb(ReadableSize::kb(0).as_mb());
    opts.set_max_total_wal_size(ReadableSize::gb(4).0);
    opts.set_max_background_jobs(6);
    opts.set_max_manifest_file_size(ReadableSize::mb(128).0);

    opts.create_if_missing(true);

    opts.set_max_open_files(40960);
    opts.enable_statistics(true);
    opts.set_stats_dump_period_sec(ReadableDuration::minutes(10).as_secs() as usize);

    opts.set_compaction_readahead_size(ReadableSize::kb(0).0);
    opts.set_max_log_file_size(ReadableSize::gb(1).0);
    opts.set_log_file_time_to_roll(ReadableDuration::secs(0).as_secs());

    opts.set_keep_log_file_num(10);
    //    if !self.info_log_dir.is_empty() {
    //        opts.create_info_log(&self.info_log_dir)
    //            .unwrap_or_else(|e| {
    //                panic!(
    //                    "create RocksDB info log {} error: {:?}",
    //                    self.info_log_dir, e
    //                );
    //            })
    //    }

    //    if self.rate_bytes_per_sec.0 > 0 {
    //        opts.set_ratelimiter_with_auto_tuned(
    //            self.rate_bytes_per_sec.0 as i64,
    //            self.rate_limiter_mode,
    //            self.auto_tuned,
    //        );
    //    }

    opts.set_bytes_per_sync(ReadableSize::mb(1).0);
    opts.set_wal_bytes_per_sync(ReadableSize::kb(512).0);
    opts.set_max_subcompactions(2);
    opts.set_writable_file_max_buffer_size(ReadableSize::mb(1).0 as i32);
    opts.set_use_direct_io_for_flush_and_compaction(false);
    opts.enable_pipelined_write(true);

    //    opts.add_event_listener(EventListener::new("kv"));

    opts
}

/// Config for default cf
pub fn default_dcf_config() -> ColumnFamilyOptions {
    let mut cf_opts = ColumnFamilyOptions::new();

    // Copy from line 344
    let mut block_base_opts = BlockBasedOptions::new();
    block_base_opts.set_block_size(ReadableSize::kb(64).0 as usize);
    block_base_opts.set_no_block_cache(false);

    let mut cache_opts = LRUCacheOptions::new();
    cache_opts
        .set_capacity(ReadableSize::mb(memory_mb_for_cf(false, CF_DEFAULT) as u64).0 as usize);

    block_base_opts.set_block_cache(&Cache::new_lru_cache(cache_opts));
    block_base_opts.set_cache_index_and_filter_blocks(true);
    block_base_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);

    block_base_opts.set_bloom_filter(10, false);
    block_base_opts.set_read_amp_bytes_per_bit(0);

    cf_opts.set_block_based_table_factory(&block_base_opts);

    let compression_per_level = vec![
        DBCompressionType::No,
        DBCompressionType::No,
        DBCompressionType::Lz4,
        DBCompressionType::Lz4,
        DBCompressionType::Lz4,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
    ];
    cf_opts.compression_per_level(compression_per_level.as_slice());
    cf_opts.set_write_buffer_size(ReadableSize::mb(128).0);
    cf_opts.set_max_write_buffer_number(5);
    cf_opts.set_min_write_buffer_number_to_merge(1);
    cf_opts.set_max_bytes_for_level_base(ReadableSize::mb(512).0);
    cf_opts.set_target_file_size_base(ReadableSize::mb(8).0);

    cf_opts.set_level_zero_file_num_compaction_trigger(4);
    cf_opts.set_level_zero_slowdown_writes_trigger(20);
    cf_opts.set_level_zero_stop_writes_trigger(36);

    cf_opts.set_max_compaction_bytes(ReadableSize::gb(2).0);
    cf_opts.compaction_priority(CompactionPriority::MinOverlappingRatio);
    cf_opts.set_level_compaction_dynamic_level_bytes(true);
    cf_opts.set_num_levels(7);
    cf_opts.set_max_bytes_for_level_multiplier(10);

    cf_opts.set_compaction_style(DBCompactionStyle::Level);
    //    cf_opts.get_disable_auto_compactions()

    cf_opts.set_soft_pending_compaction_bytes_limit(ReadableSize::gb(64).0);
    cf_opts.set_hard_pending_compaction_bytes_limit(ReadableSize::gb(256).0);

    // These line are copied from 391-402
    //    let f = Box::new(properties::RangePropertiesCollectorFactory {
    //        prop_size_index_distance: properties::DEFAULT_PROP_SIZE_INDEX_DISTANCE,
    //        prop_keys_index_distance: properties::DEFAULT_PROP_KEYS_INDEX_DISTANCE,
    //    });
    //    cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);

    cf_opts
}

pub fn default_wcf_config() -> ColumnFamilyOptions {
    // copy from 407
    let mut cf_opts = ColumnFamilyOptions::new();

    // Copy from line 344
    let mut block_base_opts = BlockBasedOptions::new();
    block_base_opts.set_block_size(ReadableSize::kb(64).0 as usize);
    block_base_opts.set_no_block_cache(false);

    let mut cache_opts = LRUCacheOptions::new();
    cache_opts
        .set_capacity(ReadableSize::mb(memory_mb_for_cf(false, CF_DEFAULT) as u64).0 as usize);

    block_base_opts.set_block_cache(&Cache::new_lru_cache(cache_opts));
    block_base_opts.set_cache_index_and_filter_blocks(true);
    block_base_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);

    block_base_opts.set_bloom_filter(10, false);
    block_base_opts.set_read_amp_bytes_per_bit(0);

    cf_opts.set_block_based_table_factory(&block_base_opts);

    let compression_per_level = vec![
        DBCompressionType::No,
        DBCompressionType::No,
        DBCompressionType::Lz4,
        DBCompressionType::Lz4,
        DBCompressionType::Lz4,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
    ];
    cf_opts.compression_per_level(compression_per_level.as_slice());

    cf_opts.set_write_buffer_size(ReadableSize::mb(128).0);
    cf_opts.set_max_write_buffer_number(5);
    cf_opts.set_min_write_buffer_number_to_merge(1);
    cf_opts.set_max_bytes_for_level_base(ReadableSize::mb(512).0);
    cf_opts.set_target_file_size_base(ReadableSize::mb(8).0);

    cf_opts.set_level_zero_file_num_compaction_trigger(4);
    cf_opts.set_level_zero_slowdown_writes_trigger(20);
    cf_opts.set_level_zero_stop_writes_trigger(36);

    cf_opts.set_max_compaction_bytes(ReadableSize::gb(2).0);
    cf_opts.compaction_priority(CompactionPriority::MinOverlappingRatio);
    cf_opts.set_level_compaction_dynamic_level_bytes(true);
    cf_opts.set_num_levels(7);
    cf_opts.set_max_bytes_for_level_multiplier(10);

    cf_opts.set_compaction_style(DBCompactionStyle::Level);
    cf_opts.set_disable_auto_compactions(false);

    //    cf_opts.get_disable_auto_compactions()

    cf_opts.set_soft_pending_compaction_bytes_limit(ReadableSize::gb(64).0);
    cf_opts.set_hard_pending_compaction_bytes_limit(ReadableSize::gb(256).0);

    //    let e = Box::new(FixedSuffixSliceTransform::new(8));
    //    cf_opts
    //        .set_prefix_extractor("FixedSuffixSliceTransform", e)
    //        .unwrap();
    // Create prefix bloom filter for memtable.
    cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
    // Collects user defined properties.
    //    let f = Box::new(properties::MvccPropertiesCollectorFactory::default());
    //    cf_opts.add_table_properties_collector_factory("tikv.mvcc-properties-collector", f);
    //    let f = Box::new(properties::RangePropertiesCollectorFactory {
    //        prop_size_index_distance: properties::DEFAULT_PROP_SIZE_INDEX_DISTANCE,
    //        prop_keys_index_distance: properties::DEFAULT_PROP_KEYS_INDEX_DISTANCE,
    //    });
    //    cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);

    cf_opts
}

"""
Metrics Exporter 并行度优化 - 代码改进方案

本文件提供了3个关键的代码改进，可以显著提升导出性能。
"""

# ========================================================================================
# 改进1: 支持告警数据的并行导出
# ========================================================================================
# 问题: export_alert_type() 不支持 parallel_degree 参数，导致告警数据无法并行导出

# 文件: metrics-exporter/metrics_exporter.py
# 位置: export_alert_type() 方法 (约第1654行)

# ❌ 原代码:
# def export_alert_type(
#     self,
#     alert_type: str,
#     config: Dict,
#     output_file: str,
#     time_range_hours: int,
#     shard_size: int = 100000,
#     batch_size: int = None,
#     source_fields: List[str] = None,
#     slim_config: SlimConfig = None,
#     mask_ip: bool = False,
#     progress_reporter: Optional[ConsoleProgressReporter] = None,
#     start_time: Optional[str] = None,
#     end_time: Optional[str] = None,
# ) -> ExportResult:

# ✅ 改进代码:
# def export_alert_type(
#     self,
#     alert_type: str,
#     config: Dict,
#     output_file: str,
#     time_range_hours: int,
#     shard_size: int = 100000,
#     batch_size: int = None,
#     source_fields: List[str] = None,
#     slim_config: SlimConfig = None,
#     mask_ip: bool = False,
#     parallel_degree: int = 1,                          # ← 新增参数
#     progress_reporter: Optional[ConsoleProgressReporter] = None,
#     start_time: Optional[str] = None,
#     end_time: Optional[str] = None,
# ) -> ExportResult:


# ========================================================================================
# 改进2: 在 export_alert_type() 方法中传递 parallel_degree
# ========================================================================================
# 文件: metrics-exporter/metrics_exporter.py
# 位置: export_alert_type() 方法体 (约第1684-1693行)

# ❌ 原代码:
#             count, file_paths = self.export_with_scroll(
#                 config["index_pattern"],
#                 query,
#                 output_file,
#                 effective_batch_size,
#                 shard_size,
#                 self._make_progress_callback(alert_type, progress_reporter),
#                 slim_config,
#                 mask_ip,
#                 # ← 这里缺少 parallel_degree 参数
#             )

# ✅ 改进代码:
#             count, file_paths = self.export_with_scroll(
#                 config["index_pattern"],
#                 query,
#                 output_file,
#                 effective_batch_size,
#                 shard_size,
#                 self._make_progress_callback(alert_type, progress_reporter),
#                 slim_config,
#                 mask_ip,
#                 parallel_degree,                        # ← 新增: 传递并行度
#             )


# ========================================================================================
# 改进3: 在导出告警数据时启用 parallel_degree
# ========================================================================================
# 文件: metrics-exporter/metrics_exporter.py
# 位置: export_all() 方法，告警数据导出逻辑 (约第1950-1965行)

# ❌ 原代码:
#             with ThreadPoolExecutor(max_workers=effective_parallel) as executor:
#                 futures = {}
#                 for alert_type in valid_alert_types:
#                     config = ALERT_TYPES[alert_type]
#                     progress_reporter.start(alert_type)
#                     output_file_base = os.path.join(output_dir, f"{alert_type}_{timestamp_suffix}")
#                     future = executor.submit(
#                         self.export_alert_type,
#                         alert_type,
#                         config,
#                         output_file_base,
#                         time_range_hours,
#                         shard_size,
#                         batch_size,
#                         source_fields,
#                         slim_config,
#                         mask_ip,
#                         progress_reporter,
#                         start_time,
#                         end_time,
#                         # ← 这里缺少 parallel_degree 参数
#                     )
#                     futures[future] = alert_type

# ✅ 改进代码:
#             with ThreadPoolExecutor(max_workers=effective_parallel) as executor:
#                 futures = {}
#                 for alert_type in valid_alert_types:
#                     config = ALERT_TYPES[alert_type]
#                     progress_reporter.start(alert_type)
#                     output_file_base = os.path.join(output_dir, f"{alert_type}_{timestamp_suffix}")
#                     future = executor.submit(
#                         self.export_alert_type,
#                         alert_type,
#                         config,
#                         output_file_base,
#                         time_range_hours,
#                         shard_size,
#                         batch_size,
#                         source_fields,
#                         slim_config,
#                         mask_ip,
#                         parallel_degree,               # ← 新增: 传递并行度
#                         progress_reporter,
#                         start_time,
#                         end_time,
#                     )
#                     futures[future] = alert_type


# ========================================================================================
# 改进4: 增加默认的批次大小（可选）
# ========================================================================================
# 文件: metrics-exporter/metrics_exporter.py
# 位置: METRIC_TYPES 定义 (约第48-90行)

# ❌ 原配置:
# METRIC_TYPES = {
#     "cluster_stats": {
#         "default_batch_size": 5000,
#     },
#     "node_stats": {
#         "default_batch_size": 3000,  # ← 太小
#     },
#     "index_stats": {
#         "default_batch_size": 3000,  # ← 太小
#     },
#     "shard_stats": {
#         "default_batch_size": 2000,  # ← 太小
#     },
# }

# ✅ 改进配置:
# METRIC_TYPES = {
#     "cluster_stats": {
#         "default_batch_size": 8000,  # ← 提升到8000
#     },
#     "node_stats": {
#         "default_batch_size": 5000,  # ← 提升到5000 (+66%)
#     },
#     "index_stats": {
#         "default_batch_size": 5000,  # ← 提升到5000 (+66%)
#     },
#     "shard_stats": {
#         "default_batch_size": 3000,  # ← 提升到3000 (+50%)
#     },
# }

# 对应的告警数据:
# ALERT_TYPES = {
#     "alert_rules": {
#         "default_batch_size": 8000,  # ← 从5000提升到8000
#     },
#     "alert_messages": {
#         "default_batch_size": 5000,  # ← 从3000提升到5000 (+66%)
#     },
#     "alert_history": {
#         "default_batch_size": 5000,  # ← 从3000提升到5000 (+66%)
#     },
# }


# ========================================================================================
# 改进5: 优化默认的并行度常数（可选）
# ========================================================================================
# 文件: common/config.py
# 位置: ExecutionConfig 类

# ❌ 原默认值:
# @dataclass
# class ExecutionConfig:
#     parallel_metrics: int = 2   # ← 太低
#     parallel_degree: int = 1    # ← 太低

# ✅ 改进默认值:
# @dataclass
# class ExecutionConfig:
#     parallel_metrics: int = 4   # ← 提升到4 (+100%)
#     parallel_degree: int = 2    # ← 提升到2 (+100%)


# ========================================================================================
# 快速部署清单
# ========================================================================================
#
# 按以下顺序应用改进，每次改进后测试性能:
#
# □ 步骤1: 改进3个并行导出相关的代码修改 (改进1、2、3)
#   - 修改 export_alert_type() 方法签名
#   - 传递 parallel_degree 参数给 export_with_scroll()
#   - 在告警导出时传递 parallel_degree 参数
#
# □ 步骤2: 增加默认批次大小 (改进4)
#   - 更新 METRIC_TYPES 的 default_batch_size
#   - 更新 ALERT_TYPES 的 default_batch_size
#
# □ 步骤3: 优化默认的并行度常数 (改进5)
#   - 更新 ExecutionConfig 的默认值
#
# □ 步骤4: 测试验证
#   - 使用高并行度测试小数据量
#   - 使用标准并行度测试大数据量
#   - 监控内存和 ES 连接数


# ========================================================================================
# 性能验收标准
# ========================================================================================
#
# 改进前基准测试（baseline）:
#   导出 100万条记录耗时: ~120秒
#   吞吐量: ~8,333 条/秒
#   资源: 4个线程, 500MB内存
#
# 改进后预期:
#   导出 100万条记录耗时: ~40-50秒   (改进 60-70%)
#   吞吐量: ~20,000-25,000 条/秒
#   资源: 12-16个线程, 1.2GB内存
#
# 如果实际改进低于预期，检查:
#   1. ES 端是否有热点 node 或高CPU
#   2. 网络带宽是否充足
#   3. 客户端内存是否已达上限
#   4. 并行度是否超过 ES 分片数


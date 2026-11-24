# Silver — trạm/ngày, lưới/HUC và trạm/tuần

Chạy từ gốc repo sau khi Bronze hoàn tất:

```sh
.venv/bin/python -m usgs_pipeline silver --config config.example.toml
.venv/bin/python -m usgs_pipeline weekly --silver-run data/medallion/silver/RUN_ID
```

Silver lưu tại `silver/data/medallion/silver/<run_id>/`; weekly tại `silver/data/medallion/weekly/<run_id>/`. Đường dẫn `data/medallion/` cũ là liên kết.

Silver lưu `station_daily/`, `stations.parquet`, `grid.parquet`, `grid_huc.parquet`,
`station_grid.parquet` và `review_unresolved.parquet`. Một dòng/trạm/ngày giữ
`discharge_cfs`, QC, ID quan sát, approval và qualifier; ngày thiếu có null.
HUC giữ dạng chuỗi và các trường không giải quyết được giữ null để rà soát.

Weekly là sản phẩm Silver dẫn xuất: `q7/q14/q28` là trung bình ngày trước thứ Ba;
`p7/p14/p28` là percentile nền 2020–2022. Có số ngày hợp lệ, số mẫu nền và lý do
thiếu; yêu cầu 4/7, 8/14, 15/28 ngày hợp lệ và đủ cửa sổ lịch. Không bù discharge.

Mỗi lần chạy tạo run riêng với manifest, checksum, schema Parquet và báo cáo.
Chỉ run `complete` đúng phương pháp được dùng ở bước tiếp theo.
Xem [hợp đồng và phương pháp](../MEDALLION.md).

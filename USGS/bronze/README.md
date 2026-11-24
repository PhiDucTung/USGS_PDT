# Bronze — nguồn USGS và trạng thái tải

Dữ liệu vật lý ở `bronze/data/`; các đường dẫn `data/` cũ là liên kết. Gồm: `raw/discovery/`, `raw/daily/`,
`catalog.json`, manifest discovery/crawl và `checkpoint.sqlite`. Raw JSON nén có
sidecar URL đã bỏ key, thời điểm tải và SHA-256; mỗi lần thử được giữ riêng.
Checkpoint là trạng thái vận hành, không phải dữ liệu thô bất biến.

`normalized/region=XX/year=YYYY/` là dữ liệu staging do crawler tạo; chứa quan sát
và cờ QC trước khi Silver hợp nhất thành một dòng/trạm/ngày. Các file đã được chuyển bằng `scripts/migrate_data_layout.py`; liên kết cũ
giữ nguyên đường dẫn mà checkpoint và manifest tham chiếu.

Từ gốc repo, chạy `discover`, `crawl`, `validate`, `audit` qua
`.venv/bin/python -m usgs_pipeline COMMAND --config config.example.toml`.
Discovery chỉ cần khi xây catalog mới; crawl tiếp tục từ checkpoint. Full crawl
cần `USGS_API_KEY` trong môi trường. Silver yêu cầu hoàn tất đúng tập job và audit raw đạt.

Xem [hướng dẫn đầy đủ](../README.md) cho pilot, retry, QC và so sánh độc lập.

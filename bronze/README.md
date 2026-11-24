# Bronze — nguồn USGS và trạng thái tải

Dữ liệu vật lý ở `bronze/data/`; các đường dẫn `data/` cũ là liên kết. Gồm: `raw/discovery/`, `raw/daily/`,
`catalog.json`, manifest discovery/crawl và `checkpoint.sqlite`. Raw JSON nén có
sidecar URL đã bỏ key, thời điểm tải và SHA-256; mỗi lần thử được giữ riêng.
Checkpoint là trạng thái vận hành, không phải dữ liệu thô bất biến.

Mã Bronze nằm trong `bronze/scripts/core.py` và `bronze/scripts/workflow.py`;
entrypoint là `bronze/scripts/run.py`. Có thể gọi từ thư mục bất kỳ:

```sh
python /Users/macbook/QTDL/USGS/bronze/scripts/run.py crawl --config config.example.toml
```

`normalized/region=XX/year=YYYY/` là dữ liệu staging do crawler tạo; chứa quan sát
và cờ QC trước khi Silver hợp nhất thành một dòng/trạm/ngày. Cấu hình mới ghi dữ
liệu trực tiếp dưới `bronze/data/`; các manifest cũ có thể còn đường dẫn của bố
cục lưu trữ trước đây.

Từ gốc repo, chạy `discover`, `crawl`, `validate`, `audit` qua
`.venv/bin/python bronze/scripts/run.py COMMAND --config config.example.toml`
(hoặc `python -m usgs_pipeline COMMAND` nếu cần dùng giao diện package).
Discovery chỉ cần khi xây catalog mới; crawl tiếp tục từ checkpoint. Full crawl
cần `USGS_API_KEY` trong môi trường. Silver yêu cầu hoàn tất đúng tập job và audit raw đạt.

Xem [hướng dẫn đầy đủ](../README.md) cho pilot, retry, QC và so sánh độc lập.

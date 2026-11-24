# USGS CONUS daily discharge, 2020–2022

Pipeline Python lấy lưu lượng **trung bình ngày** (`00060` / `00003`) từ USGS Water Data OGC API v1. Tập trạm được xây dựng từ metadata, gồm cả trạm lịch sử có thời gian khả dụng giao với giai đoạn yêu cầu; lọc CONUS bằng HUC2 `01`–`18`. Không đặt chỉ tiêu phải đủ 23.084 trạm. Gold tính percentile hồi cứu với nền 2020–2022; repo chưa cung cấp percentile theo nền lịch sử 1990–2020.

## Cài đặt

Python 3.11 trở lên, macOS/Linux (khóa tiến trình dùng `fcntl`).

```sh
python3 -m venv .venv
.venv/bin/pip install -e '.[test]'
```

Đăng ký key tại https://api.waterdata.usgs.gov/signup/ rồi đặt `USGS_API_KEY` trong môi trường shell hoặc secret manager. Pipeline truyền key bằng header `X-Api-Key`, không dùng tham số URL. Không cần key để thử ít truy vấn, nhưng lượt tải lớn nên có key riêng. Không lưu key trong file cấu hình.

Nguồn chính thức:

- API: https://api.waterdata.usgs.gov/docs/ogcapi/
- Key/rate limit: https://api.waterdata.usgs.gov/docs/ogcapi/keys/

## Quy trình

```sh
# 1. Tạo catalog từ tất cả metadata lưu lượng trung bình ngày
.venv/bin/python -m usgs_pipeline discover --config config.example.toml

# 2. Pilot tối đa 20 trạm, chọn luân phiên qua các vùng có trạm phù hợp
.venv/bin/python -m usgs_pipeline crawl --config config.example.toml --limit-sites 20

# 3. Xem báo cáo pilot, kiểm tra độ phủ, lỗi, số trang và dung lượng
.venv/bin/python -m usgs_pipeline validate --config config.example.toml

# 4. Tải toàn bộ và chạy lại cùng lệnh để retry/resume
.venv/bin/python -m usgs_pipeline crawl --config config.example.toml

# 5. Xuất CSV theo trạm; Parquet đã được tạo khi crawl
.venv/bin/python -m usgs_pipeline export --config config.example.toml
```

`validate` trả exit code 2 khi còn công việc lỗi/chưa tải/chưa lập lịch; điều này là bình thường sau pilot. `crawl` trả 2 nếu còn công việc chưa thành công, 1 khi lỗi cấu hình/xác thực, 130 khi bị ngắt. Chạy lại `crawl` sẽ bỏ qua `complete`/`empty`, tải lại toàn bộ công việc `running`/`error` và giữ các bản raw cũ. Không thay đổi date/region trong cùng thư mục output; dùng output mới cho phạm vi mới. Chỉ một lệnh được dùng một output tại một thời điểm.

Có thể chọn cụ thể trạm đã có trong catalog:

```sh
.venv/bin/python -m usgs_pipeline crawl --config config.example.toml --sites USGS-01646500,USGS-09380000
```

Pilot ưu tiên phủ vùng, sau đó bổ sung một trạm có chuỗi kết thúc trước năm hiện tại nếu còn chỗ. Cách chọn này không đảm bảo tự chọn được đủ mọi loại trạm. Dựa vào `catalog.json` và báo cáo pilot, dùng `--sites` tải bổ sung trạm có kết thúc chuỗi trước hiện tại và trạm có khoảng trống; không suy diễn rằng chuỗi đã kết thúc đồng nghĩa trạm hiện đã đóng. Nếu vùng không có trạm đủ điều kiện, báo cáo đúng thực tế.

## Dữ liệu bàn giao

Trong `bronze/data/` (đổi bằng `--output` hoặc TOML):

- `catalog.json`: một dòng logic cho mỗi chuỗi, gồm trạm, tên, tọa độ `[longitude, latitude]`, HUC, vùng, đơn vị, begin/end, quyết định chọn/loại. `missing_or_invalid_huc` cần kiểm tra thủ công, không bị loại âm thầm.
- `discovery_manifest.json`, `crawl_manifest.json`: phạm vi, thời điểm và danh sách trạm đã chọn.
- `checkpoint.sqlite`: công việc trạm × năm, trạng thái, lỗi, số dòng, số trang, dung lượng raw và thời gian worker.
- `raw/discovery/`, `raw/daily/`: phản hồi JSON nén và sidecar `.meta.json` có URL đã bỏ key, thời điểm tải, SHA-256 của file nén. Mỗi lần thử có thư mục riêng.
- `normalized/region=XX/year=YYYY/USGS-XXXXXXXX.parquet`: schema cố định; dùng `pyarrow.parquet.ParquetFile(path).read()` khi đọc một file trong thư mục phân vùng. `region` và `year` cũng có trong dữ liệu.
- `reports/quality.json`: độ phủ, khoảng ngày thiếu theo năm, số dòng xung đột, số giá trị không hợp lệ/đơn vị lạ, tổng hợp vùng, công việc lỗi và công việc chưa được lập lịch.
- `reports/unresolved_jobs.json`: danh sách công việc chưa thành công.
- `csv/`: CSV theo trạm, nhập mã trạm bằng kiểu text để giữ số 0 đầu.

`all_jobs_successful` chỉ đánh giá tính đầy đủ của việc tải tập trạm đã khám phá, không khẳng định trạm đủ ngày hoặc chất lượng mọi giá trị đều tốt. Có thể hoàn tất tải nhưng trạm thiếu nhiều ngày. Số ngày kỳ vọng là ngày lịch toàn khoảng yêu cầu: 1.096 ngày cho 2020–2022, gồm 29/02/2020.

`estimate` ngoại suy chi phí còn lại từ các job đã tải và chỉ là ước tính lý tưởng; không gồm chờ hạn mức hoặc retry trong tương lai. `elapsed_worker_seconds` là tổng thời gian của các công việc đã hoàn tất, không phải wall clock của toàn lượt tải. Dùng trung bình thời gian/trang/dung lượng từ pilot để ước tính quy mô; các job lỗi và lần thử cũ vẫn lưu raw nhưng không cộng vào metrics lượt thành công.

## Quy tắc dữ liệu

- Giữ ngày USGS cung cấp, không chuyển thành ngày UTC; không nội suy hoặc thay missing bằng 0.
- Giữ `raw_value`, `approval_status`, `qualifier`, `last_modified`, ID quan sát và ID chuỗi.
- `discharge_cfs` chỉ có số hữu hạn và đơn vị cfs nhận diện được. Đơn vị lạ hoặc giá trị chữ được giữ ở trường gốc và gắn cờ; số âm được giữ nguyên để rà soát theo ngữ cảnh trạm.
- ID quan sát lặp y hệt được gộp; cùng ID nhưng nội dung khác gây lỗi công việc để tránh mất phiên bản âm thầm.
- Nhiều quan sát trạm/ngày có giá trị hoặc đơn vị khác nhau được giữ nguyên và `conflict=true`. Không tự ưu tiên chuỗi hoặc lấy trung bình.
- Ranh giới khả dụng metadata chỉ dùng chọn trạm; crawl vẫn yêu cầu toàn khoảng ngày mục tiêu của mỗi trạm để phát hiện khoảng trống.
- Discovery chỉ công bố catalog sau khi tải hết các trang; nếu discovery gián đoạn, chạy lại từ đầu. Khả năng resume theo job áp dụng cho phần crawl.
- Snapshot là dữ liệu lịch sử **được USGS cung cấp lúc tải**, có thể đã được chỉnh sửa sau 2022. Không phải bản dữ liệu được đóng băng trong năm 2022.

## Đối chiếu dữ liệu tải thủ công

Tải daily values từ USGS cho ít nhất 5 trạm, chọn cùng khoảng ngày và mã `00060`/`00003`, chuẩn hóa file tham chiếu thành CSV:

```csv
site_id,date,discharge_cfs
USGS-01646500,2020-02-29,9620
```

File thực tế phải có ít nhất 5 mã trạm khác nhau. Giữ file ZIP/CSV tải gốc cùng thời điểm tải để đối chiếu nguồn. Lệnh sau so sánh từng dòng tham chiếu, chấp nhận một trong các giá trị nếu trạm/ngày có nhiều chuỗi, và ghi toàn bộ sai lệch:

```sh
.venv/bin/python -m usgs_pipeline compare --config config.example.toml --reference /absolute/path/reference.csv
```

Kết quả ở `reports/manual_comparison.json`, gồm checksum file tham chiếu. Kiểm thử tự động của tính năng compare dùng fixture tổng hợp; chỉ được coi là đối chiếu thủ công thực tế khi đã cung cấp file tải độc lập. Việc so sánh này không kiểm tra các ngày không có trong file tham chiếu.

## Kiểm thử

```sh
.venv/bin/python -m pytest -q
```

Bộ kiểm thử dùng HTTP mock, không gọi mạng: phân trang, 429/5xx, Retry-After, lỗi kết nối, dừng xác thực, chống rò key, URL phân trang ngoài USGS, tải dở không commit, resume, duplicate, xung đột, zero/missing/đơn vị lạ, năm nhuận, HUC và đối chiếu 5 trạm.

## Quy trình toàn bộ trước khi tiền xử lý

Sau khi cấu hình `USGS_API_KEY` trong **chính môi trường chạy lệnh**, dùng:

```sh
cd /Users/macbook/QTDL/USGS
.venv/bin/python -m usgs_pipeline run --config config.example.toml --max-donors 20
```

`run` thực hiện theo thứ tự: kiểm tra dữ liệu hiện có → crawl toàn bộ catalog (không discovery lại) → validate → kiểm tra checksum raw → export CSV → preprocess. Nếu toàn bộ dữ liệu đã được kiểm định thành công, không cần key và không gọi mạng. Nếu còn cần crawl mà thiếu key, lệnh dừng và ghi lý do vào `bronze/data/reports/workflow_status.json`. Chạy lại cùng lệnh để tiếp tục; các job `complete`/`empty` được giữ nguyên. Full `crawl` cũng yêu cầu key; chế độ pilot hoặc chọn trạm riêng vẫn dùng được không có key.

Khi một job đã ghi `complete`/`empty` nhưng Parquet bị hỏng, workflow sẽ chặn tiền xử lý. Cần khôi phục file hoặc đánh dấu đúng job lỗi để tải lại; không xóa toàn bộ checkpoint. Không có vòng lặp tự động vô hạn khi xác thực hoặc kiểm định thất bại.

Có thể chạy riêng:

```sh
.venv/bin/python -m usgs_pipeline audit --config config.example.toml
.venv/bin/python -m usgs_pipeline preprocess --config config.example.toml --max-donors 20
```

`audit` kiểm tra checksum mọi file raw hiện có và sự hiện diện của bộ trang raw cho các job đã hoàn tất. `preprocess` bắt buộc validate đạt cho **đúng tập trạm × năm**, bao gồm kiểm tra region/start/end/status/row count của job và checksum raw; pilot chưa tải toàn bộ bị chặn. Cổng này không yêu cầu trạm có đủ quan sát mỗi ngày: job rỗng hợp lệ vẫn được xử lý thành chuỗi ngày thiếu.

### Phương pháp tiền xử lý

Đây là phương pháp **tại trạm** điều chỉnh từ `fillmissingflow.m`, không tái lập phương pháp percentile trên lưới của paper. Không tải nền 1990–2020, không tính rolling/percentile, không tạo giá trị bù.

- Mỗi trạm/ngày có một dòng; giữ 0 và giá trị âm. Các quan sát hợp lệ cùng giá trị được gộp và giữ danh sách ID nguồn.
- Cờ QC phân biệt `missing_day`, `invalid_numeric`, `unexpected_unit`, `conflict`. Ngày có xung đột hoặc dữ liệu không hợp lệ có giá trị sử dụng được là null; dữ liệu gốc không bị sửa.
- HUC4/6/8 lấy từ tiền tố HUC nguồn, giữ mã dạng chuỗi. Metadata thiếu/mâu thuẫn được báo cáo và không dùng tại cấp bị ảnh hưởng.
- Ứng viên phải là trạm khác, cùng HUC tương ứng, có quan sát hợp lệ **đúng ngày**. Chọn tối đa 20 sau khi xét tính hợp lệ theo ngày, xếp theo Haversine km rồi mã trạm; không đặt bán kính bổ sung.
- Các cấp HUC được xét độc lập; một donor có thể xuất hiện ở nhiều cấp. Chưa chọn cấp ưu tiên và không lấy trung bình các donor.
- Tính bằng quan sát sẵn có; không dùng dữ liệu bù làm nguồn. Trạng thái duyệt và qualifier được giữ, không mặc định loại quan sát có qualifier ước tính.

Mỗi lần chạy thành công tạo `silver/data/preprocessed/<run_id>/` mới:

| Đầu ra | Nội dung |
|---|---|
| `daily/region=XX/year=YYYY/USGS-....parquet` | Một dòng/trạm/ngày: `site_id`, `date`, `discharge_cfs`, `qc_flags`, `source_observation_ids`, `approval_status`, `qualifier`. |
| `candidates/year=YYYY/part.parquet` | Trạm đích, ngày, cấp/mã HUC, donor, giá trị cfs quan trắc, khoảng cách km, rank và ID nguồn. |
| `candidate_summary/year=YYYY/part.parquet` | Tổng số donor đủ điều kiện trước khi giới hạn, số giữ lại và lý do không có donor. |
| `report.json` | Số ngày sử dụng được/thiếu theo cờ, số ứng viên, vấn đề metadata; `filled_values=0`. |
| `manifest.json` | Cấu hình, thời điểm, phương pháp, checksum catalog và tất cả Parquet nguồn, trạng thái chạy, trạng thái báo cáo đối chiếu độc lập. |

Bảng ngày được xử lý theo trạm/năm; một SQLite tạm trên đĩa hỗ trợ tìm donor theo từng ngày. Các bảng ứng viên được ghi theo batch để không giữ toàn bộ kết quả trong RAM. File tạm được dọn khi kết thúc; output bị gián đoạn có manifest `incomplete` và không được coi là kết quả hoàn tất. Mỗi lần chạy lại tiền xử lý tạo run mới.

`reports/workflow_status.json` ghi tiến trình workflow, `reports/raw_audit.json` ghi kết quả checksum. Trường `manual_comparison_completed=false` được giữ khi chưa có báo cáo đối chiếu độc lập đạt. Thành công của workflow tự động không thay thế đối chiếu tải thủ công ít nhất 5 trạm; tiếp tục dùng lệnh `compare` ở phần trên.

## Giảm các đợt chờ do hạn mức API

Cấu hình mặc định hiện là `requests_per_second = 0.25`: một request mỗi 4 giây, tối đa khoảng 900 request/giờ dùng chung cho mọi worker. Đây là cách tải đều hơn, không tăng hạn mức USGS hoặc đảm bảo không bao giờ gặp 429. Các lần retry cũng đi qua bộ giới hạn tốc độ.

Khi nhận 429, pipeline lưu `Retry-After` thành thời điểm được phép gọi lại trong `bronze/data/reports/rate_limit.json`. Khởi động lại vẫn tôn trọng thời điểm đó. Thay đổi TOML chỉ có hiệu lực với tiến trình mới.

Lượt chạy sau khi điều chỉnh tốc độ ghi stdout/stderr vào `bronze/data/logs/crawl.log`. Xem trực tiếp bằng:

```sh
tail -f /Users/macbook/QTDL/USGS/bronze/data/logs/crawl.log
```

Giảm tốc độ chỉ giảm khả năng gặp các đợt chờ dài trong tương lai; khoảng chờ USGS đã yêu cầu vẫn phải được tôn trọng.

## Tự chạy từ Terminal bằng script

```sh
cd /Users/macbook/QTDL/USGS
./run_crawl.sh status
./run_crawl.sh
```

Script tự vào thư mục dự án, dùng `.venv`, kiểm tra pipeline đang chạy và nhận key qua biến môi trường hoặc lời nhắc nhập ẩn. Không lưu key vào file. Workflow tiếp tục từ checkpoint và tôn trọng cooldown đã lưu; stdout/stderr vừa hiển thị trong Terminal vừa được nối vào `bronze/data/logs/crawl.log`.

Nếu crawler trong Codex vẫn đang chạy, script từ chối chạy thêm. Cần kết thúc tiến trình cũ trước khi chuyển sang Terminal. Giữ Terminal và máy hoạt động trong khi crawl; đóng Terminal, sleep hoặc tắt máy có thể ngắt tiến trình. Chạy lại cùng script để resume.

## Bronze → Silver → Gold (2020–2022)

Pipeline Python bổ sung các lệnh `silver`, `weekly`, `gold`, `validate-gold`,
`build-gold`. Hướng dẫn, schema và các khác biệt với paper: [MEDALLION.md](MEDALLION.md).
Baseline cố định 2020–2022 phục vụ phân tích hồi cứu; giữ mọi phương án mean/IDW
HUC8/6/4. Cần trích lưới gốc trước khi chạy `build-gold`.


## Bản đồ kiến trúc theo tầng

| Tầng | Dữ liệu thực tế | Hướng dẫn |
|---|---|---|
| Bronze | `bronze/data/`: raw, normalized, catalog, checkpoint, dữ liệu nguồn và báo cáo crawl | [Bronze](bronze/README.md) |
| Silver | `silver/data/`: preprocessed, medallion/silver và medallion/weekly | [Silver](silver/README.md) |
| Gold | `gold/data/medallion/`: gold, gold_filled và gold_local_20x20 | [Gold](gold/README.md) |

Các thư mục `bronze/`, `silver/`, `gold/` chứa dữ liệu thật và tài liệu theo tầng.
Cấu hình mới dùng `bronze/data/`; dữ liệu đã phân tầng vật lý và không còn thư mục
`data/` ở gốc. `usgs_pipeline/` giữ mã thực thi; các file dữ liệu ở ba tầng được
`.gitignore` loại khỏi commit. Lệnh `run` kết thúc
ở tiền xử lý tại trạm; dùng `build-gold` riêng để tạo sản phẩm lưới.

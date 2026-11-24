# Gold — chỉ số dòng chảy theo ô lưới/tuần

```sh
.venv/bin/python -m usgs_pipeline gold --weekly-run data/medallion/weekly/RUN_ID
.venv/bin/python -m usgs_pipeline validate-gold --gold-run data/medallion/gold/RUN_ID
```

Đầu ra vật lý ở `gold/data/medallion/gold/<run_id>/` (có liên kết tại `data/medallion/gold/<run_id>/`): `grid_weekly/`, `lineage/`,
`report.json` và `manifest.json`. Khóa duy nhất là `(grid_id, week_date)`.
Mỗi tuần có toàn bộ lưới đầu vào; 156 thứ Ba trong 2020–2022.

`p7/p14/p28` là trung bình percentile các trạm gán trực tiếp vào ô. Ô thiếu có
các phương án `mean_huc*` và `idw_huc*` độc lập tại HUC8/6/4, kèm số donor và
lý do thiếu. Percentile hữu hạn trong [0,100]; null không được đổi thành 0.
Lineage lưu trạm, percentile nguồn và khoảng cách; manifest dẫn về daily và raw.

Gold tiêu chuẩn giữ các phương án HUC. Công cụ `fill_gold.py` hiện có tạo sản phẩm
bù riêng theo cửa sổ 20×20 và ưu tiên HUC8→6→4; xem [MEDALLION](../MEDALLION.md).
Các giá trị bù không làm donor và không sửa discharge gốc.

Báo cáo kiểm tra lịch, lưới, tái tổng hợp từ trạm, lineage, độ phủ và phân bố số
nguồn đóng góp. `automated_checks_only` khác `complete`: cần báo cáo so sánh USGS
độc lập đạt để nghiệm thu đầy đủ. Nền 2020–2022 chỉ phục vụ phân tích hồi cứu.

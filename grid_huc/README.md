# Tạo ánh xạ lưới USDM – HUC

## Lưới gốc của paper (đã chọn cho dự án)

Dùng `USDMDataAvg.csv`, phiên bản Dryad ngày 14/03/2024:
https://datadryad.org/dataset/doi:10.5061/dryad.g1jwstqw7 .
File có kích thước 1.588.620.226 byte; SHA-256 theo metadata Dryad:
`ae7da84c03dbd0573b32b80f823503bd4fad7ce294e5f5b496f4ddace155910f`.
Ngày 15/11/2026, tải tự động trả HTTP 403; API download trả 401.
Cần có file CSV tải thành công trước khi trích lưới. Công cụ dưới đây đọc toàn bộ
CSV theo từng phần, giữ nguyên `grid`, `lon`, `lat`, loại dòng lặp và kiểm tra ID
mâu thuẫn; không tái tạo lưới từ khoảng cách 0,5°.

Chạy từ thư mục gốc `/Users/macbook/QTDL`:

```bash
.venv/bin/python grid_huc/prepare_paper_inputs.py grid --csv doi_10_5061_dryad_g1jwstqw7__v20240314/USDMDataAvg.csv
.venv/bin/python grid_huc/prepare_paper_inputs.py wbd --repair-invalid
.venv/bin/python grid_huc/build_grid_huc.py --grid bronze/data/grid_huc_inputs/grid.csv --wbd bronze/data/grid_huc_inputs/wbd/WBDHU8.gpkg --layer WBDHU8 --out silver/data/grid_huc_output
```

Lệnh `wbd` lấy snapshot layer HUC-8 của dịch vụ USGS, vùng HUC 01–18; lưu từng
nhóm GeoJSON để chạy tiếp khi tải gián đoạn, kiểm tra đủ object ID, không yêu cầu
đơn giản hóa hình học và lưu metadata/checksum trong `wbd_source.json`.
Snapshot dịch vụ hiện tại không bảo đảm giống phiên bản WBD tác giả đã dùng.
`--repair-invalid` cho phép `make_valid` và ghi từng polygon đã sửa trong
metadata. Công cụ dừng nếu kết quả không còn là polygon hoặc thay đổi diện tích
tương đối vượt 0,000001 (đo trong EPSG:5070). GeoJSON gốc vẫn được giữ để đối chiếu.
Lệnh trích lưới mặc định kiểm tra checksum của đúng bản Dryad 14/03/2024.
Chỉ xem là tải hoàn tất khi có cả `WBDHU8.gpkg` và `wbd_source.json`.
`verification_report.json` ghi trạng thái kiểm tra, không phải kết quả ánh xạ.

## Đầu vào bắt buộc
1. CSV lưới USDM đã dùng trong dự án: `grid_id,lon,lat` (tâm ô, WGS84/EPSG:4326). Có thể dùng bảng USDM theo tuần: script chỉ lấy ID/toạ độ và bỏ các bản sao giống hệt. Không dùng catalog trạm làm lưới. Không tự tạo ID vì có thể lệch USDM/NLDAS hiện có.
2. Polygon WBD HUC-8 bao phủ toàn miền nghiên cứu. Tải từ https://apps.nationalmap.gov/downloader/ (Watershed Boundary Dataset). Giải nén, dùng layer `WBDHU8` trong File Geodatabase, hoặc shapefile/GeoPackage tương ứng. Chỉ shapefile .shp là chưa đủ: giữ .dbf, .shx, .prj cùng bộ. Không dùng polygon HUC-12 dưới tên field HUC8.
   Hướng dẫn nguồn: https://www.usgs.gov/national-hydrography/access-national-hydrography-products

Catalog USGS cung cấp HUC của trạm, không thay thế polygon HUC hay danh sách tâm lưới. Bộ đính kèm hiện chưa có hai đầu vào trên, nên chưa thể tạo bảng ánh xạ thật cho dự án.

## Chạy
```bash
python -m pip install -r requirements.txt
python build_grid_huc.py --grid grid.csv --wbd WBD_National.gdb --layer WBDHU8 --out grid_huc_output
```
Nếu CSV có cột `grid,longitude,latitude`:
```bash
python build_grid_huc.py --grid usdm_weekly.csv --id-col grid --lon-col longitude --lat-col latitude --wbd WBD_National.gdb --layer WBDHU8 --out grid_huc_output
```
Shapefile: thay `--wbd WBD_National.gdb --layer WBDHU8` bằng `--wbd WBDHU8.shp`.
Tên field HUC mặc định `huc8`, không phân biệt hoa/thường; thay bằng `--huc-field TEN_COT` nếu cần.

## Phương pháp và kiểm tra
- Đọc HUC dạng chuỗi đúng 8 chữ số. Từ chối mã số đã mất zero đầu; không tự đoán hoặc thêm zero.
- Giữ nguyên tâm/ID lưới. Chuyển điểm WGS84 sang CRS khai báo của polygon trước khi spatial join; không chỉ đổi nhãn CRS.
- Dissolve các phần có cùng HUC8, tránh trùng do multipart. Dừng với geometry hỏng/thiếu CRS để sửa có kiểm soát.
- `inside`: tâm nằm trong đúng một HUC. `boundary_unique`: chỉ một HUC tiếp xúc chính xác tâm trên biên.
- `boundary_ambiguous`, `overlap_ambiguous`: có nhiều ứng viên. Giữ null và danh sách ứng viên, không chọn ngẫu nhiên. Paper không công bố quy tắc xử lý điểm đúng biên.
- `unmatched`: không có HUC bao phủ tâm. Kiểm tra độ phủ WBD (đặc biệt biên giới/ven biển), CRS và toạ độ; không tự gán HUC gần nhất.
- Lấy 6/4 ký tự đầu của HUC8 để có các cấp cha cùng phiên bản WBD. Không trộn HUC từ nhiều phiên bản mà không kiểm tra.
- Mã thoát 2: đã xuất kết quả, nhưng còn điểm chưa giải quyết. `--allow-unresolved` chỉ đổi mã thoát về 0, không điền mã HUC. Chỉ dùng nếu pipeline chủ động giữ null cho các ô đó.
- HUC được gán theo TÂM ô, không theo lưu vực chiếm diện tích lớn nhất. Đây là phương pháp paper mô tả. Không bảo đảm tái tạo y hệt bản đồ tác giả nếu bản WBD/lưới khác phiên bản.
- Không sửa tọa độ hoặc chọn lưu vực chỉ để hết missing; quyết định xử lý biên phải được ghi lại và áp dụng nhất quán.

## Đầu ra
- `grid_huc.csv`, `grid_huc.parquet`: `grid_id,Lon,Lat,huc8,huc6,huc4,match_status,candidate_huc8`.
- `review_unresolved.csv`: các ô cần kiểm tra.
- `qa_report.json`: số ô từng trạng thái, CRS, đường dẫn nguồn, hash CSV và phiên bản GeoPandas. Giữ thêm bộ WBD gốc và metadata ngày tải để tái lập.
- Output folder phải mới/rỗng để tránh ghi đè nhầm.

## Dùng với MATLAB của tác giả
Chạy `load_gridHUCs.m` từ thư mục chứa `grid_huc_output`, rồi `load('gridHUCs.mat')` trước `fillMissingFlows.m`.
Bảng có đúng cột `Lat,Lon,huc4,huc6,huc8` mà script tác giả truy vấn. Chuẩn hoá `stationHUCs.huc4/huc6/huc8` thành string tương ứng (ví dụ `"0101"`) để so sánh với bảng mới. Không để một bên numeric, một bên string.
Script import dừng nếu còn ô chưa có HUC. Nếu chọn giữ null có chủ đích, phải sửa bước bù thiếu để bỏ qua các ô đó thay vì tìm HUC bằng null.

## Kiểm chứng
Chạy kiểm thử: `python test_grid_huc.py`. Bộ test tổng hợp kiểm tra tâm trong polygon, biên chung, biên ngoài, ô không được bao phủ, polygon chồng lấn, chuyển CRS, zero đầu, dòng lặp và ID mâu thuẫn. Chưa kiểm chứng trên polygon WBD và lưới thật của bạn.

Nguồn phương pháp: Erhardt et al. (2024), https://doi.org/10.1038/s41597-024-03202-6, phần USGS Streamflow; https://github.com/heplersa/USDMdata/blob/main/fillMissingFlows.m.
API spatial join: https://geopandas.org/en/stable/docs/reference/api/geopandas.sjoin.html

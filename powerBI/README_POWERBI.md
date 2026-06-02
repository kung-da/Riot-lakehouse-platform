# Riot Dashboard Power BI

Đây là project Power BI dạng **PBIP/PBIR** cho Gold layer của Riot Lakehouse Platform.

## Nội dung chính

- `Riot-dashboard.pbip`: file mở project bằng Power BI Desktop.
- `Riot-dashboard.Report/`: report pages, theme, visual containers.
- `Riot-dashboard.SemanticModel/`: semantic model TMDL, tables, relationships, DAX measures.
- `Riot-dashboard.pbix`: file PBIX hiện có, không chỉnh trực tiếp bằng script.

## Trạng thái hiện tại

- Gold model đã có đủ 15 bảng Gold.
- Measures đã được đưa vào semantic model.
- Relationships đã được chuẩn hóa theo star schema.
- Theme dark Riot/esports đã được áp vào report.
- Report có 7 trang:
  - Executive Overview
  - Player Performance
  - Champion Meta
  - Role Lane Analysis
  - Rank Snapshot
  - Team Objectives
  - Timeline Events
- Visual containers đã được sinh trong PBIR ở từng page.

## Cách mở

Mở file này bằng Power BI Desktop:

`powerBI/Riot-dashboard.pbip`

Nếu Power BI Desktop đang mở project cũ, hãy đóng rồi mở lại để Desktop đọc toàn bộ file JSON/TMDL mới.

## Lưu ý

PBIP/PBIR là dạng text project nên có thể sửa bằng code. PBIX là binary/proprietary, không nên chỉnh trực tiếp bằng script vì dễ làm hỏng file.

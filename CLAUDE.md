#  Tổng quan  dự án

## 1. Tổng quan dự án

- **Tên đề tài:** Hệ thống gợi ý phim thời gian thực trên nền tảng dữ liệu lớn
- **Use case:** Recommendation Engine (E-commerce / Media Streaming / Social Networks).
- **Kiến trúc:** **Lambda Architecture** (Batch + Speed + Serving).
- **Thuật toán cốt lõi:** **ALS** (Alternating Least Squares) — Collaborative Filtering, Spark MLlib.
- **3 nguồn dữ liệu:** MovieLens 25M (lịch sử) · **Wikimedia EventStreams (real-time THẬT)** · TMDB (poster).
---

## 2. Stack công nghệ

| Layer | Công nghệ | Vai trò trong dự án |
|-------|-----------|---------------------|
| Ingestion | **Kafka** + Zookeeper | Topic `ratings-stream`: nhận rating replay + sự kiện phim Wikipedia live |
| Data Lake | **HDFS** | Lưu `ratings.csv` 647MB + `movies.csv` |
| Batch | **Spark (PySpark) + MLlib** | Train ALS, sinh Top-N, phim phổ biến |
| Speed/Real-time view | **HBase** | Gợi ý real-time theo user (row=userId) + thịnh hành (row `__TRENDING__`) |
| Batch view | **MySQL** | `movies(+poster_url)`, `user_recommendations`, `popular_movies`, `stats` |
| Orchestration | **Airflow** | DAG: load HDFS → train → verify |
| Serving | **Flask** | Web demo + REST API (`/api/recommend/<id>`, `/api/trending`) |
| Deploy | **Docker Compose** | 13 container |
| Nguồn real-time | **Wikimedia EventStreams** | SSE công khai live, không key (= vai trò WebSocket Binance) |
| Làm giàu | **TMDB API** | Poster phim thật qua `tmdbId` |

---

## 3. Cấu trúc thư mục

```
BIG DATA/
├── docker-compose.yml          # 13 service (zookeeper,kafka,namenode,datanode,spark-master,
│                               #   spark-worker,hbase,mysql,airflow,ingestion,wiki,stream,serving)
├── .env                        # Kafka/MySQL/ALS/HBase + STREAM_RATE_PER_SEC + (TMDB_API_KEY không commit)
├── Tổng Quan.md                   # tổng quan dự án
├── data/ml-25m/                # MovieLens 25M (KHÔNG commit, tự tải vì nặng không commit được): ratings/movies/links/tags/genome...
├── ingestion/
│   ├── download_data.py        # tải MovieLens
│   ├── stream_producer.py      # replay rating → Kafka (MÔ PHỎNG)
│   └── wiki_stream_producer.py # Wikimedia EventStreams → Kafka 
├── batch/train_als.py          # PySpark ALS: train, RMSE/MAE, Top-N, phim phổ biến
├── stream/consumer.py          # Kafka → HBase: gợi ý thể loại (user) + trending (Wikimedia)
├── serving/
│   ├── app.py                  # Flask: index/recommend/search/health + /api/trending + poster
│   ├── templates/              # base,index,recommend,search,_card.html
│   └── static/style.css
├── scripts/
│   ├── load_to_hdfs.sh · mysql_init.sql · build_demo_db.py
│   ├── fetch_posters.py        # lấy poster TMDB → MySQL.poster_url
│   ├── make_charts.py          # sinh biểu đồ , được lưu trong docs/images/*.png
│   ├── build_user_history.py · run_local.ps1
├── airflow/dags/recsys_pipeline.py
└── docs/
    └── images/                 # rating_distribution / top_genres / top_popular .png
```



## 4. Kết quả thực nghiệm (chạy thật 25M)

- **RMSE = 0.7965 · MAE = 0.6187** (test 20% trên **25.000.095** ratings) — ngang/vượt benchmark MovieLens.
- **400.000** gợi ý (20.000 user × Top-20), lọc phim ≥1.000 lượt (MIN_REC_RATINGS=1000) → gợi ý kinh điển.
- ALS: `rank=64, maxIter=10, regParam=0.08, coldStartStrategy=drop`. Users ~162.541, phim 62.423.
- **11.866 poster** TMDB; trending Wikimedia khớp ~**2 phim/phút** (thật).
- Demo: `http://localhost:5000`. URL admin: HDFS :9870 · Spark :8080 · HBase :16010 · Airflow :8088 (admin/admin).













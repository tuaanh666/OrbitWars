"""
wiki_stream_producer.py — Nguồn REAL-TIME THẬT cho hệ thống gợi ý phim.
Thay cho việc replay dataset, file này cắm vào **Wikimedia EventStreams**
(https://stream.wikimedia.org/v2/stream/recentchange) — một luồng SSE CÔNG KHAI,
chảy LIVE 24/7, miễn phí, không cần API key .

Mỗi khi có ai đó **chỉnh sửa một trang phim trên Wikipedia ngay lúc này**,  ta coi là
một tín hiệu phim đang được quan tâm và đẩy sự kiện vào Kafka. Stream layer sẽ tổng hợp
thành danh sách " Thịnh hành real-time".

Cách dùng:
    python wiki_stream_producer.py             # chạy thật, đẩy Kafka
    python wiki_stream_producer.py --test       # CHỈ in phim khớp (không cần Kafka) để kiểm thử
    python wiki_stream_producer.py --test --seconds 80
"""
import os
import re
import csv
import json
import sys
import time
import argparse
import urllib.request

STREAM_URL = os.environ.get(
    "WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC_RATINGS", "ratings-stream")
MOVIES_CSV = os.environ.get("MOVIES_PATH", "/app/data/ml-25m/movies.csv")
WIKI = os.environ.get("WIKI_FILTER", "enwiki")   

_ARTICLE_RE = re.compile(r"^(.*),\s+(The|A|An|La|Le|Les|Il|El|Die|Das|Der|Une|Un)$", re.I)


def canon(title: str) -> str:
    """Chuẩn hoá tiêu đề về dạng so khớp: bỏ năm, đảo ', The' -> 'The ', bỏ dấu câu."""
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", title).strip()    
    m = _ARTICLE_RE.match(t)
    if m:
        t = f"{m.group(2)} {m.group(1)}"                    
    t = t.lower()
    t = re.sub(r"[^a-z0-9 ]", " ", t)                      
    t = re.sub(r"\s+", " ", t).strip()
    return t


def canon_wiki(title: str) -> str:
    """Tiêu đề Wikipedia: bỏ hậu tố '(film)' / '(2010 film)' rồi chuẩn hoá."""
    t = re.sub(r"\s*\((?:\d{4} )?film\)\s*$", "", title, flags=re.I)
    t = re.sub(r"\s*\((?:\d{4} )?(?:American |British )?film\)\s*$", "", t, flags=re.I)
    return canon(t)


def load_movie_index(path):
    """canonical title -> (movieId, fullTitle). Bỏ phim trùng tên (giữ id nhỏ nhất)."""
    index = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = canon(row["title"])
            if not key:
                continue
            mid = int(row["movieId"])
            if key not in index or mid < index[key][0]:
                index[key] = (mid, row["title"])
    print(f"[OK] Nạp {len(index):,} tên phim (canonical) từ {path}")
    return index


def iter_stream(url):
    """Đọc luồng SSE, yield từng object JSON (dòng 'data:')."""
    req = urllib.request.Request(url, headers={"User-Agent": "movielens-recsys/1.0"})
    resp = urllib.request.urlopen(req, timeout=30)
    for raw in resp:
        line = raw.decode("utf-8", "ignore").strip()
        if line.startswith("data:"):
            payload = line[5:].strip()
            if payload:
                try:
                    yield json.loads(payload)
                except json.JSONDecodeError:
                    continue


def run(test=False, seconds=0):
    movies = load_movie_index(MOVIES_CSV)

    producer = None
    if not test:
        from kafka import KafkaProducer
        for attempt in range(30):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: str(k).encode("utf-8"),
                )
                print(f"[OK] Kết nối Kafka {KAFKA_BROKER}")
                break
            except Exception as e:
                print(f"[..] Chờ Kafka ({attempt+1}/30): {e}")
                time.sleep(5)
        if producer is None:
            raise RuntimeError("Không kết nối được Kafka")

    print(f"[..] Lắng nghe Wikimedia EventStreams (live) — lọc {WIKI}, namespace bài viết...")
    start = time.time()
    seen, matched = 0, 0
    while True:
        try:
            for ev in iter_stream(STREAM_URL):
                seen += 1
                if ev.get("wiki") != WIKI or ev.get("namespace") != 0:
                    continue
                if ev.get("type") not in ("edit", "new"):
                    continue
                key = canon_wiki(ev.get("title", ""))
                hit = movies.get(key)
                if not hit:
                    continue
                matched += 1
                movie_id, full_title = hit
                event = {
                    "movieId": movie_id,
                    "title": full_title,
                    "event_type": "trending",
                    "source": "wikipedia",
                    "wiki_title": ev.get("title"),
                    "wiki_user": ev.get("user"),
                    "timestamp": int(time.time()),
                }
                if test:
                    print(f"  🎬 [{matched}] LIVE: '{ev.get('title')}' -> "
                          f"movieId={movie_id} ({full_title}) | bởi {ev.get('user')}")
                else:
                    producer.send(TOPIC, key=movie_id, value=event)
                    if matched % 10 == 0:
                        producer.flush()
                        print(f"  -> Đã đẩy {matched} sự kiện phim (live) | đã quét {seen} thay đổi")

                if seconds and (time.time() - start) >= seconds:
                    print(f"\n[OK] Kết thúc test sau {seconds}s: quét {seen} thay đổi, "
                          f"khớp {matched} phim.")
                    return
        except Exception as e:
            print(f"[!!] Mất kết nối stream ({e}); kết nối lại sau 3s...")
            time.sleep(3)
            if seconds and (time.time() - start) >= seconds:
                return


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true", help="Chỉ in phim khớp, không cần Kafka")
    ap.add_argument("--seconds", type=int, default=0, help="Dừng sau N giây (cho test)")
    args = ap.parse_args()
    run(test=args.test, seconds=args.seconds)

from flask import Flask, request, jsonify
from datetime import datetime
import json
import os
import requests
from threading import Lock

app = Flask(__name__)

# 동기화를 위한 락
lock = Lock()

# 좌표 군집화 함수 (0.001 단위 반올림)
def round_coord(lat, lon):
    try:
        return f"{round(float(lat), 3)},{round(float(lon), 3)}"
    except:
        return "null,null"

# 좌표 그룹별 카운트 업데이트
def update_group_counter(coord):
    counter_file = "group_counter.txt"
    lock.acquire()
    try:
        counts = {}
        if os.path.exists(counter_file):
            with open(counter_file, "r") as f:
                for line in f:
                    k, v = line.strip().split(":")
                    counts[k] = int(v)
        counts[coord] = counts.get(coord, 0) + 1
        with open(counter_file, "w") as f:
            for k, v in counts.items():
                f.write(f"{k}:{v}\n")
    finally:
        lock.release()

# 사용자 경로 업데이트
def update_user_path(agent_id, coord):
    os.makedirs("user_paths", exist_ok=True)
    file_path = f"user_paths/{agent_id}.txt"
    with open(file_path, "a") as f:
        f.write(coord + "\n")

# 자주 등장한 좌표 확인 (100 이상)
def is_frequent(coord):
    try:
        with open("group_counter.txt", "r") as f:
            for line in f:
                key, count = line.strip().split(":")
                if key == coord and int(count) >= 100:
                    return True
    except:
        pass
    return False

# GPS 데이터 수신
@app.route('/api/v1/gps', methods=['POST'])
def receive_gps():
    try:
        data = request.get_json(force=True)
        required_fields = {"trip_id", "agent_id", "latitude", "longitude", "timestamp"}
        if not all(field in data for field in required_fields):
            return jsonify({"error": "Missing field(s)"}), 400

        lat = data["latitude"]
        lon = data["longitude"]
        coord = round_coord(lat, lon)

        update_group_counter(coord)
        update_user_path(data["agent_id"], coord)

        print(f"{datetime.now()} - Received: {data}")
        return jsonify({"status": "received"})
    except Exception as e:
        print("Error:", e)
        return jsonify({"error": str(e)}), 500

# OSRM 매핑 및 캐싱 API
@app.route('/api/v1/route', methods=['POST'])
def get_route():
    try:
        data = request.get_json(force=True)
        start = data.get("start")  # [lat, lon]
        end = data.get("end")      # [lat, lon]

        if not start or not end:
            return jsonify({"error": "Missing start or end"}), 400

        rounded_start = round_coord(start[0], start[1])
        rounded_end = round_coord(end[0], end[1])
        route_key = f"{rounded_start}__{rounded_end}"
        cache_file = f"cache_routes/{route_key}.json"

        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                return jsonify(json.load(f))

        # OSRM 서버에 요청
        url = f"http://localhost:5000/route/v1/driving/{start[1]},{start[0]};{end[1]},{end[0]}?overview=full"
        response = requests.get(url)

        if response.status_code == 200:
            result = response.json()
            if is_frequent(rounded_start) and is_frequent(rounded_end):
                os.makedirs("cache_routes", exist_ok=True)
                with open(cache_file, "w") as f:
                    json.dump(result, f)
            return jsonify(result)
        else:
            return jsonify({"error": "OSRM 요청 실패"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8081)

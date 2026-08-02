import sys, cv2, os, json, psutil, gc, threading, queue

os.environ["OMP_NUM_THREADS"] = "1"
os.environ["TF_NUM_INTRAOP_THREADS"] = "1"
os.environ["TF_NUM_INTEROP_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["GLOG_minloglevel"] = "3"
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["OPENCV_LOG_LEVEL"] = "OFF"

MOUTH_INDICES = [61, 146, 91, 181, 84, 17, 314, 405, 321, 375, 291, 409, 270, 269, 267, 0, 37, 39, 40, 185, 78, 95, 88, 178, 87, 14, 317, 402, 318, 324, 308, 415, 310, 311, 312, 13, 82, 81, 80, 191]

def send_ipc(data):
    print(f"ECOFLOW_IPC__{json.dumps(data)}", flush=True)

def reader_thread(q):
    for line in iter(sys.stdin.readline, ''):
        q.put(line)
    q.put(None)

def boot_daemon():
    process = psutil.Process(os.getpid())
    send_ipc({"status": "ready", "actual_ram_mb": process.memory_info().rss / (1024 * 1024)})

    input_queue = queue.Queue()
    t = threading.Thread(target=reader_thread, args=(input_queue,))
    t.daemon = True
    t.start()

    IDLE_TIMEOUT_SECONDS = 60

    while True:
        try:
            line = input_queue.get(timeout=IDLE_TIMEOUT_SECONDS)
            if line is None: break
            line = line.strip()
            if not line: continue

            req = json.loads(line)
            input_target = req.get("input")
            chunk_name = os.path.basename(input_target)

            targets = []
            if os.path.isdir(input_target):
                targets = [os.path.join(input_target, f) for f in sorted(os.listdir(input_target)) if f.endswith("_landmarks.json")]
            else: targets = [input_target]

            total = len(targets)
            for idx, landmarks_json in enumerate(targets):
                try:
                    img_path = landmarks_json.replace("_landmarks.json", ".jpg")
                    if os.path.exists(img_path):
                        with open(landmarks_json, "r") as f: all_landmarks = json.load(f)
                        img = cv2.imread(img_path)
                        
                        if img is not None and img.var() > 1.0 and img.shape[0] >= 64 and img.shape[1] >= 64 and all_landmarks:
                            height, width = img.shape[:2]
                            for face_idx, points in enumerate(all_landmarks):
                                mouth_points = [points[i] for i in MOUTH_INDICES if i < len(points)]
                                if not mouth_points: continue

                                xs, ys = [p[0] for p in mouth_points], [p[1] for p in mouth_points]
                                center_x, center_y = sum(xs) // len(xs), sum(ys) // len(ys)
                                half_size = 48
                                x1, y1 = max(0, center_x - half_size), max(0, center_y - half_size)
                                x2, y2 = min(width, center_x + half_size), min(height, center_y + half_size)

                                crop = img[y1:y2, x1:x2]
                                if crop.size > 0:
                                    cv2.imwrite(os.path.join(input_target, f"{os.path.splitext(os.path.basename(img_path))[0]}_mouth_face_{face_idx}.jpg"), cv2.resize(crop, (96, 96)))
                                
                                del crop
                        if img is not None:
                            del img
                except Exception:
                    pass

                if (idx + 1) % 15 == 0 or (idx + 1) == total:
                    send_ipc({"status": "progress", "chunk": chunk_name, "pct": int(((idx + 1) / total) * 100)})

            gc.collect()
            send_ipc({"status": "success", "chunk": input_target})

        except queue.Empty:
            send_ipc({"status": "warn", "message": f"🧹 MouthIsolator idle for {IDLE_TIMEOUT_SECONDS}s. Self-terminating to release RAM."})
            break
            
        except Exception as e:
            send_ipc({"status": "error", "error": str(e)})

if __name__ == "__main__": boot_daemon()
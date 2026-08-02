import sys, cv2, os, json, psutil, gc, threading, queue

os.environ["OMP_NUM_THREADS"] = "1"
os.environ["TF_NUM_INTRAOP_THREADS"] = "1"
os.environ["TF_NUM_INTEROP_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["GLOG_minloglevel"] = "3"
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["OPENCV_LOG_LEVEL"] = "OFF"

def send_ipc(data):
    print(f"ECOFLOW_IPC__{json.dumps(data)}", flush=True)

def reader_thread(q):
    for line in iter(sys.stdin.readline, ''):
        q.put(line)
    q.put(None)

def boot_daemon():
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    model_path = os.path.join(project_root, "models", "face_detection_yunet_2023mar.onnx")

    face_detector = cv2.FaceDetectorYN.create(model=model_path, config="", input_size=(320, 320), score_threshold=0.8, nms_threshold=0.3, top_k=5000)

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
                targets = [os.path.join(input_target, f) for f in sorted(os.listdir(input_target)) if f.startswith("frame_") and not "_mouth" in f and f.endswith((".jpg", ".png"))]
            else: targets = [input_target]

            total = len(targets)
            
            for idx, img_path in enumerate(targets):
                try:
                    img = cv2.imread(img_path)
                    if img is not None and img.var() > 1.0 and img.shape[0] >= 64 and img.shape[1] >= 64:
                        height, width = img.shape[:2]
                        face_detector.setInputSize((width, height))
                        _, faces = face_detector.detect(img)

                        output_data = [{"x": int(f[0]), "y": int(f[1]), "w": int(f[2]), "h": int(f[3])} for f in faces] if faces is not None else []
                        with open(os.path.splitext(img_path)[0] + "_faces.json", "w") as f:
                            json.dump(output_data, f)
                        del faces
                    if img is not None:
                        del img
                except Exception:
                    pass 

                if (idx + 1) % 15 == 0 or (idx + 1) == total:
                    send_ipc({"status": "progress", "chunk": chunk_name, "pct": int(((idx + 1) / total) * 100)})

            gc.collect() 
            send_ipc({"status": "success", "chunk": input_target})

        except queue.Empty:
            send_ipc({"status": "warn", "message": f"🧹 FaceDetector idle for {IDLE_TIMEOUT_SECONDS}s. Self-terminating to release RAM."})
            break
            
        except Exception as e:
            send_ipc({"status": "error", "error": str(e)})

if __name__ == "__main__": boot_daemon()
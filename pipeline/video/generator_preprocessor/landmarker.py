import sys, cv2, mediapipe as mp, os, json, psutil, gc, threading, queue
from mediapipe.tasks import python
from mediapipe.tasks.python import vision

os.environ["OMP_NUM_THREADS"] = "1"
os.environ["TF_NUM_INTRAOP_THREADS"] = "1"
os.environ["TF_NUM_INTEROP_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["GLOG_minloglevel"] = "3"
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["OPENCV_LOG_LEVEL"] = "OFF"

def send_ipc(data):
    print(f"ECHOFLOW_IPC__{json.dumps(data)}", flush=True)

def reader_thread(q):
    for line in iter(sys.stdin.readline, ''):
        q.put(line)
    q.put(None)

def boot_daemon():
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    model_path = os.path.join(project_root, "models", "face_landmarker.task")

    base_opts = python.BaseOptions(model_asset_path=model_path, delegate=python.BaseOptions.Delegate.CPU)
    options = vision.FaceLandmarkerOptions(base_options=base_opts, num_faces=10)
    
    detector = vision.FaceLandmarker.create_from_options(options)

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
                        rgb_img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
                        mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=rgb_img)
                        detection_result = detector.detect(mp_image)
                        height, width = img.shape[:2]

                        all_landmarks = [[[int(pt.x * width), int(pt.y * height)] for pt in face_landmarks] for face_landmarks in detection_result.face_landmarks] if detection_result.face_landmarks else []
                        with open(os.path.splitext(img_path)[0] + "_landmarks.json", "w") as f:
                            json.dump(all_landmarks, f)

                        del rgb_img, mp_image, detection_result
                    
                    if img is not None:
                        del img
                except Exception:
                    pass

                if (idx + 1) % 15 == 0 or (idx + 1) == total:
                    send_ipc({"status": "progress", "chunk": chunk_name, "pct": int(((idx + 1) / total) * 100)})

            gc.collect() 
            send_ipc({"status": "success", "chunk": input_target})

        except queue.Empty:
            send_ipc({"status": "warn", "message": f"🧹 FacialLandmarker idle for {IDLE_TIMEOUT_SECONDS}s. Self-terminating to release RAM."})
            break
            
        except Exception as e:
            send_ipc({"status": "error", "error": str(e)})

if __name__ == "__main__": boot_daemon()
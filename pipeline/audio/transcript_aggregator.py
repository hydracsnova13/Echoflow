import sys
import os
import json

def aggregate_transcripts(input_path, output_dir):
    # 1. Dynamically resolve the JOB-XXX root folder
    curr = os.path.abspath(input_path)
    job_root = None
    
    while curr and os.path.dirname(curr) != curr:
        # Check if we are already inside the job folder
        if os.path.exists(os.path.join(curr, "out_AudioChunker")):
            job_root = curr
            break
        # Fallback to checking the folder name
        if os.path.basename(curr).startswith("JOB-"):
            job_root = curr
            break
        curr = os.path.dirname(curr)
        
    if not job_root:
        job_root = input_path

    audio_chunker_dir = os.path.join(job_root, "out_AudioChunker")
    
    if not os.path.exists(audio_chunker_dir):
        print(f"❌ [TranscriptAggregator] Audio chunker directory missing at {audio_chunker_dir}", flush=True)
        return

    # 2. Extract and strictly sort buckets to maintain chronological order
    buckets = sorted([
        os.path.join(audio_chunker_dir, d) for d in os.listdir(audio_chunker_dir)
        if d.startswith("bucket_") and os.path.isdir(os.path.join(audio_chunker_dir, d))
    ])

    if not buckets:
        print(f"❌ [TranscriptAggregator] No buckets found in {audio_chunker_dir}", flush=True)
        return

    master_timeline = []
    full_transcript_parts = []
    language = "en"

    # 3. Aggregate all JSONs
    for b_dir in buckets:
        t_file = os.path.join(b_dir, "transcript.json")
        if os.path.exists(t_file):
            try:
                with open(t_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    language = data.get("language", language)

                    # Prioritize granular segment micro-data
                    if "segments" in data and data["segments"]:
                        for seg in data["segments"]:
                            master_timeline.append(seg)
                    else:
                        # Fallback if micro-data is missing
                        if data.get("text"):
                            master_timeline.append({
                                "start": data.get("start", 0.0),
                                "end": data.get("end", 0.0),
                                "text": data.get("text")
                            })

                    if data.get("text"):
                        full_transcript_parts.append(data["text"])
            except Exception as e:
                print(f"⚠️ [TranscriptAggregator] Failed to parse {t_file}: {str(e)}", flush=True)

    # Enforce strict chronological ordering to fix overlapping chunks
    master_timeline.sort(key=lambda x: x["start"])

    payload = {
        "job_id": os.path.basename(job_root),
        "language": language,
        "global_transcript": " ".join(full_transcript_parts),
        "timeline": master_timeline
    }

    # 4. Save to the targeted out_TranscriptAggregator directory
    target_path = os.path.join(output_dir, "master_transcript.json")
    os.makedirs(output_dir, exist_ok=True)
    
    with open(target_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"✅ [TranscriptAggregator] Master timed transcript saved to {target_path}", flush=True)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        # sys.argv[1] = inputFile passed by Go
        # sys.argv[2] = outputDir passed by Go
        aggregate_transcripts(sys.argv[1], sys.argv[2])
    else:
        print("❌ [TranscriptAggregator] Missing input/output arguments from Orchestrator.", flush=True)
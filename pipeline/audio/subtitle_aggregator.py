import sys
import os
import json

def format_timestamp(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    milliseconds = int((seconds - int(seconds)) * 1000)
    return f"{hours:02}:{minutes:02}:{secs:02},{milliseconds:03}"

def generate_srt(timeline: list, output_path: str):
    with open(output_path, "w", encoding="utf-8") as f:
        for idx, segment in enumerate(timeline, start=1):
            start_time = format_timestamp(segment.get("start", 0.0))
            end_time = format_timestamp(segment.get("end", 0.0))
            text = segment.get("translated_text", "")
            if text:
                f.write(f"{idx}\n{start_time} --> {end_time}\n{text}\n\n")

def aggregate_subtitles(input_path, output_dir):
    curr = os.path.abspath(input_path)
    job_root = None
    
    # Dynamically find the active JOB root
    while curr and os.path.dirname(curr) != curr:
        if os.path.exists(os.path.join(curr, "out_AudioChunker")):
            job_root = curr
            break
        if os.path.basename(curr).startswith("JOB-"):
            job_root = curr
            break
        curr = os.path.dirname(curr)
        
    if not job_root:
        job_root = input_path

    audio_chunker_dir = os.path.join(job_root, "out_AudioChunker")
    
    if not os.path.exists(audio_chunker_dir):
        print(f"❌ [SubtitleAggregator] Cannot find chunk directory at {audio_chunker_dir}", flush=True)
        return

    buckets = sorted([
        os.path.join(audio_chunker_dir, d) for d in os.listdir(audio_chunker_dir)
        if d.startswith("bucket_") and os.path.isdir(os.path.join(audio_chunker_dir, d))
    ])

    master_timeline = []
    full_transcript_parts = []
    target_language = "hi"

    for b_dir in buckets:
        t_file = os.path.join(b_dir, "translated.json")
        if os.path.exists(t_file):
            try:
                with open(t_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    target_language = data.get("language", target_language)

                    if "segments" in data and data["segments"]:
                        for seg in data["segments"]:
                            if seg.get("translated_text"):
                                master_timeline.append(seg)
                                full_transcript_parts.append(seg["translated_text"])
            except Exception as e:
                print(f"⚠️ [SubtitleAggregator] Failed to parse {t_file}: {str(e)}", flush=True)

    # Resolve overlapping chunk timestamps by strict chronological sorting
    master_timeline.sort(key=lambda x: x["start"])

    payload = {
        "job_id": os.path.basename(job_root),
        "target_language": target_language,
        "global_translation": " ".join(full_transcript_parts),
        "timeline": master_timeline
    }

    os.makedirs(output_dir, exist_ok=True)
    
    json_out = os.path.join(output_dir, "master_translated.json")
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        
    srt_out = os.path.join(output_dir, "subtitles.srt")
    generate_srt(master_timeline, srt_out)

    print(f"✅ [SubtitleAggregator] JSON and Subtitles generated at {output_dir}", flush=True)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        # sys.argv[1] is automatically injected by the Go DAGExecutor as the input path
        # sys.argv[2] is injected as the output directory
        aggregate_subtitles(sys.argv[1], sys.argv[2])
    else:
        print("❌ [SubtitleAggregator] Missing arguments from DAG Engine.")
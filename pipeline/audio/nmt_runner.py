import sys
import os
import json
import hashlib
from pathlib import Path

os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TRANSFORMERS_VERBOSITY"] = "error"

def format_timestamp(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    milliseconds = int((seconds - int(seconds)) * 1000)
    return f"{hours:02}:{minutes:02}:{secs:02},{milliseconds:03}"

class OfflineTransformersEngine:
    LANGUAGE_CODE_MAP = {"en": "eng_Latn", "hi": "hin_Deva", "mr": "mar_Deva"}

    def __init__(self, model_dir: str):
        self.model_dir = model_dir
        self.tokenizer = None
        self.model = None

    def load(self) -> None:
        if self.model is None or self.tokenizer is None:
            import logging
            logging.getLogger("transformers").setLevel(logging.ERROR)
            import torch
            from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
            
            torch.set_num_threads(os.cpu_count() or 4)
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_dir, local_files_only=True)
            self.model = AutoModelForSeq2SeqLM.from_pretrained(self.model_dir, local_files_only=True)

    def translate_batch(self, texts: list, source_language: str, target_language: str) -> list:
        self.load()
        src_code = self.LANGUAGE_CODE_MAP.get(source_language, "eng_Latn")
        tgt_code = self.LANGUAGE_CODE_MAP.get(target_language, "hin_Deva")
        self.tokenizer.src_lang = src_code
        self.tokenizer.tgt_lang = tgt_code

        forced_bos_token_id = self.tokenizer.convert_tokens_to_ids(tgt_code)
        if forced_bos_token_id == self.tokenizer.unk_token_id:
            forced_bos_token_id = self.tokenizer.convert_tokens_to_ids(f"<{tgt_code}>")

        # 🛡️ THE OPTIMIZATION: Process all texts inside a single padded tensor matrix
        inputs = self.tokenizer(texts, return_tensors="pt", padding=True, truncation=True)
        
        # 🛡️ THE OPTIMIZATION: Beam Search, Repetition Penalty, and Length normalization
        generated_tokens = self.model.generate(
            **inputs, 
            forced_bos_token_id=forced_bos_token_id, 
            max_length=512,
            num_beams=4,
            repetition_penalty=1.2,
            no_repeat_ngram_size=3
        )
        
        return self.tokenizer.batch_decode(generated_tokens, skip_special_tokens=True)

def run_nmt(input_target: str, output_dir: str):
    project_root = Path(__file__).resolve().parent.parent.parent
    model_dir = str(project_root / "models" / "nllb-200-distilled-600M")

    transcript_file = input_target
    if os.path.isdir(input_target):
        possible_json = os.path.join(input_target, "master_transcript.json")
        if os.path.exists(possible_json):
            transcript_file = possible_json
        else:
            for root, _, files in os.walk(input_target):
                for f in files:
                    if f == "master_transcript.json" or f.endswith(".json"):
                        transcript_file = os.path.join(root, f)
                        break

    if not os.path.exists(transcript_file):
        print(f"❌ [NMTRunner] Input file not found: {transcript_file}", flush=True)
        sys.exit(1)

    curr = os.path.abspath(transcript_file)
    job_root = None
    while curr and os.path.dirname(curr) != curr:
        if os.path.basename(curr).startswith("JOB-"):
            job_root = curr
            break
        curr = os.path.dirname(curr)

    target_language = "hi"
    if job_root:
        config_path = os.path.join(job_root, "job_config.json")
        if os.path.exists(config_path):
            with open(config_path, "r") as cfg:
                target_language = json.load(cfg).get("target_language", "hi")

    with open(transcript_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    timeline = data.get("timeline", [])
    source_language = data.get("language", "en")
    engine = OfflineTransformersEngine(model_dir)

    print(f"🔍 [NMTRunner] Source: {source_language.upper()} ➔ Target: {target_language.upper()}", flush=True)

    requests = []
    for idx, seg in enumerate(timeline):
        src_text = seg.get("text", "")
        if source_language == target_language:
            timeline[idx]["translated_text"] = src_text
        else:
            requests.append({"idx": idx, "src_text": src_text})

    if requests:
        print(f"   ⚡ Batch translating {len(requests)} segments simultaneously...", flush=True)
        texts_to_translate = [req["src_text"] for req in requests]
        
        translated_texts = engine.translate_batch(texts_to_translate, source_language, target_language)
        
        for req, trans in zip(requests, translated_texts):
            timeline[req["idx"]]["translated_text"] = trans

    os.makedirs(output_dir, exist_ok=True)
    json_out = os.path.join(output_dir, "master_translated.json")
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # 🛡️ THE FIX: Output BOTH Source and Target subtitles
    srt_out_target = os.path.join(output_dir, "subtitles.srt")
    with open(srt_out_target, "w", encoding="utf-8") as f:
        for idx, seg in enumerate(timeline, start=1):
            start_str = format_timestamp(float(seg.get("start", 0.0)))
            end_str = format_timestamp(float(seg.get("end", 0.0)))
            text = seg.get("translated_text", "")
            speaker_id = seg.get("speaker_id", "SPEAKER_00")
            f.write(f"{idx}\n{start_str} --> {end_str}\n[{speaker_id}] {text}\n\n")

    srt_out_source = os.path.join(output_dir, "source.srt")
    with open(srt_out_source, "w", encoding="utf-8") as f:
        for idx, seg in enumerate(timeline, start=1):
            start_str = format_timestamp(float(seg.get("start", 0.0)))
            end_str = format_timestamp(float(seg.get("end", 0.0)))
            text = seg.get("text", "")
            speaker_id = seg.get("speaker_id", "SPEAKER_00")
            f.write(f"{idx}\n{start_str} --> {end_str}\n[{speaker_id}] {text}\n\n")

    print(f"✅ [NMTRunner] Execution complete!", flush=True)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        run_nmt(sys.argv[1], sys.argv[2])
    else:
        print("❌ [NMTRunner] Missing required arguments: input_path output_dir", flush=True)
        sys.exit(1)
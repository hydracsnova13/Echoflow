import sys
import os
import json
import re
import gc
import time
from pathlib import Path

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

num_cores = min(4, os.cpu_count() or 4)
os.environ["OMP_NUM_THREADS"] = str(num_cores)
os.environ["OPENBLAS_NUM_THREADS"] = str(num_cores)
os.environ["MKL_NUM_THREADS"] = str(num_cores)
os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TRANSFORMERS_VERBOSITY"] = "error"

import torch
torch.set_num_threads(num_cores)

from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
try:
    from IndicTransToolkit import IndicProcessor
except ImportError:
    from IndicTransToolkit.IndicTransToolkit import IndicProcessor

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
DICT_FILE = CONFIG_DIR / "domain_dictionary.json"

ASR_CORRECTIONS = {}
ASR_STEM_PATTERNS = []
ENGLISH_SPOKEN_SMOOTHING = []
HINDI_SPOKEN_SMOOTHING = []
MARATHI_SPOKEN_SMOOTHING = []

def load_domain_dictionary():
    global ASR_CORRECTIONS, ASR_STEM_PATTERNS, ENGLISH_SPOKEN_SMOOTHING, HINDI_SPOKEN_SMOOTHING, MARATHI_SPOKEN_SMOOTHING
    if DICT_FILE.exists():
        try:
            with open(DICT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                ASR_CORRECTIONS = data.get("asr_corrections", {})
                stem_list = data.get("asr_stem_patterns", [])
                ASR_STEM_PATTERNS = [
                    (re.compile(item["pattern"], re.IGNORECASE | re.UNICODE), item["replacement"])
                    for item in stem_list if "pattern" in item and "replacement" in item
                ]
                en_list = data.get("spoken_english_smoothing", [])
                hi_list = data.get("spoken_hindi_smoothing", [])
                mr_list = data.get("spoken_marathi_smoothing", [])
                ENGLISH_SPOKEN_SMOOTHING = [
                    (item["pattern"], item["replacement"])
                    for item in en_list if "pattern" in item and "replacement" in item
                ]
                HINDI_SPOKEN_SMOOTHING = [
                    (item["pattern"], item["replacement"])
                    for item in hi_list if "pattern" in item and "replacement" in item
                ]
                MARATHI_SPOKEN_SMOOTHING = [
                    (item["pattern"], item["replacement"])
                    for item in mr_list if "pattern" in item and "replacement" in item
                ]
        except Exception as e:
            print(f"⚠️ [NMTTranslator] Could not load {DICT_FILE}: {e}", flush=True)

load_domain_dictionary()

FOREIGN_SCRIPTS_PATTERN = re.compile(
    r"[\u0A00-\u0A7F\u0C00-\u0C7F\u0C80-\u0CFF\u0B80-\u0BFF\u0D00-\u0D7F\u0980-\u09FF\u0600-\u06FF\u0E00-\u0E7F]"
)

POSTPOSITIONS = [
    "मध्ये", "साठी", "पासून", "पर्यंत", "पेक्षा", "समोर", "मागे", "पुढे", 
    "खाली", "वर", "कडे", "द्वारे", "मुळे", "प्रमाणे", "नुसार", "बद्दल",
    "च्या", "चे", "ची", "चा", "ना", "ने", "नी", "स", "ला"
]

DIALECT_PARTICLES = [
    (r'(?<![\u0900-\u097F])अनी(?![\u0900-\u097F])', 'आणि'),
    (r'(?<![\u0900-\u097F])आनिन(?![\u0900-\u097F])', 'आणि'),
    (r'(?<![\u0900-\u097F])आन्नी(?![\u0900-\u097F])', 'आणि'),
    (r'(?<![\u0900-\u097F])मदे(?![\u0900-\u097F])', 'मध्ये'),
    (r'(?<![\u0900-\u097F])मादे(?![\u0900-\u097F])', 'मध्ये'),
    (r'(?<![\u0900-\u097F])साति(?![\u0900-\u097F])', 'साठी'),
    (r'(?<![\u0900-\u097F])सती(?![\u0900-\u097F])', 'साठी'),
    (r'(?<![\u0900-\u097F])साटी(?![\u0900-\u097F])', 'साठी'),
    (r'(?<![\u0900-\u097F])मुले(?![\u0900-\u097F])', 'मुळे'),
    (r'(?<![\u0900-\u097F])मनुन(?![\u0900-\u097F])', 'म्हणून'),
    (r'(?<![\u0900-\u097F])अता(?![\u0900-\u097F])', 'आता')
]

def sanitize_nmt_input(text: str, source_lang: str) -> str:
    if not text:
        return ""
    if source_lang and source_lang.lower() in ["mr", "hi"]:
        cleaned = FOREIGN_SCRIPTS_PATTERN.sub("", text)
        return re.sub(r"\s+", " ", cleaned).strip()
    return text.strip()

def clean_asr_text(text: str) -> str:
    if not text:
        return ""
    cleaned = text
    cleaned = re.sub(r'(\w{2,})(?:\s+\1){2,}', r'\1', cleaned, flags=re.UNICODE)
    cleaned = re.sub(r'(.{3,}?)\1{3,}', r'\1', cleaned, flags=re.UNICODE)

    for pattern, replacement in ASR_STEM_PATTERNS:
        cleaned = pattern.sub(replacement, cleaned)

    sorted_corrections = sorted(ASR_CORRECTIONS.items(), key=lambda x: len(x[0]), reverse=True)
    for target, replacement in sorted_corrections:
        pattern = r"(?<![\u0900-\u097F])" + re.escape(target) + r"(?![\u0900-\u097F])"
        cleaned = re.sub(pattern, replacement, cleaned)

    for pat, repl in DIALECT_PARTICLES:
        cleaned = re.sub(pat, repl, cleaned)

    pp_pattern = r'(\b[\u0900-\u097F]{2,})\s+(' + '|'.join(POSTPOSITIONS) + r')(?=[\s.,!?।॥]|$)'
    cleaned = re.sub(pp_pattern, r'\1\2', cleaned)
    cleaned = re.sub(pp_pattern, r'\1\2', cleaned)

    cleaned = re.sub(r'\s+([.,!?।॥])', r'\1', cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()

def smooth_natural_english(text: str) -> str:
    smoothed = text
    for pattern, replacement in ENGLISH_SPOKEN_SMOOTHING:
        smoothed = re.sub(pattern, replacement, smoothed, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", smoothed).strip()

def smooth_natural_hindi(text: str) -> str:
    smoothed = text
    for pattern, replacement in HINDI_SPOKEN_SMOOTHING:
        smoothed = re.sub(pattern, replacement, smoothed, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", smoothed).strip()

def smooth_natural_marathi(text: str) -> str:
    smoothed = text
    for pattern, replacement in MARATHI_SPOKEN_SMOOTHING:
        smoothed = re.sub(pattern, replacement, smoothed, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", smoothed).strip()

def format_timestamp(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    milliseconds = int((seconds - int(seconds)) * 1000)
    return f"{hours:02}:{minutes:02}:{secs:02},{milliseconds:03}"

class OfflineIndicTransEngine:
    LANGUAGE_CODE_MAP = {
        "en": "eng_Latn",
        "hi": "hin_Deva",
        "mr": "mar_Deva",
        "ta": "tam_Taml",
        "te": "tel_Telu",
        "kn": "kan_Knda",
        "gu": "guj_Gujr",
        "bn": "ben_Beng",
        "pa": "pan_Guru",
        "ur": "urd_Arab",
        "sa": "san_Deva",
        "ne": "npi_Deva",
        "or": "ory_Orya",
        "as": "asm_Beng",
        "mai": "mai_Deva"
    }

    def __init__(self, models_root: Path):
        self.models_root = models_root
        self.current_model_dir = None
        self.tokenizer = None
        self.model = None
        self.ip = None

    def get_model_flavor(self, src_code: str, tgt_code: str) -> str:
        if tgt_code == "eng_Latn":
            return "indictrans2-indic-en-1B"
        elif src_code == "eng_Latn":
            return "indictrans2-en-indic-1B"
        else:
            return "indictrans2-indic-indic-1B"

    def load(self, model_dir_name: str) -> None:
        if self.current_model_dir == model_dir_name and self.model is not None:
            return

        if self.model is not None:
            print(f"🧹 [NMTTranslator] Flushing previous model from RAM...", flush=True)
            del self.model
            del self.tokenizer
            del self.ip
            gc.collect()

        model_path = self.models_root / model_dir_name
        if not model_path.exists():
            raise FileNotFoundError(f"IndicTrans2 model directory not found at: {model_path}")

        print(f"📦 [NMTTranslator] Loading IndicTrans2 '{model_dir_name}' into RAM...", flush=True)
        self.tokenizer = AutoTokenizer.from_pretrained(
            str(model_path),
            trust_remote_code=True,
            local_files_only=True
        )
        self.model = AutoModelForSeq2SeqLM.from_pretrained(
            str(model_path),
            trust_remote_code=True,
            local_files_only=True
        ).to("cpu")
        self.ip = IndicProcessor(inference=True)
        self.current_model_dir = model_dir_name
        print(f"✅ [NMTTranslator] Model initialized successfully.", flush=True)

    def translate_batch(self, texts: list, source_language: str, target_language: str) -> list:
        if not texts:
            return []

        src_code = self.LANGUAGE_CODE_MAP.get(source_language.lower(), source_language)
        tgt_code = self.LANGUAGE_CODE_MAP.get(target_language.lower(), target_language)

        model_flavor = self.get_model_flavor(src_code, tgt_code)
        self.load(model_flavor)

        mini_batch_size = 2
        translations = []

        for start in range(0, len(texts), mini_batch_size):
            chunk = texts[start:start + mini_batch_size]
            done_n = min(start + mini_batch_size, len(texts))
            
            print(f"   🔄 [NMTTranslator] Translating Batch {done_n}/{len(texts)}...", flush=True)
            batch = self.ip.preprocess_batch(chunk, src_lang=src_code, tgt_lang=tgt_code)
            
            inputs = self.tokenizer(
                batch,
                truncation=True,
                padding="longest",
                return_tensors="pt",
                return_attention_mask=True
            ).to("cpu")
            
            attn = inputs.get("attention_mask")
            input_length = inputs["input_ids"].shape[1]
            dynamic_max_tokens = min(256, int(input_length * 2.5) + 20)

            gen_start_time = time.time()
            with torch.no_grad():
                generated_tokens = self.model.generate(
                    **inputs,
                    use_cache=True,
                    max_new_tokens=dynamic_max_tokens,
                    num_beams=3,
                    do_sample=False,
                    repetition_penalty=1.1,         # 🛡️ THE FIX: Soft penalty to prevent Hindi NMT loops
                    no_repeat_ngram_size=3,         # 🛡️ THE FIX: Forces model to stop repeating 3-word chunks
                    num_return_sequences=1
                )
            gen_duration = time.time() - gen_start_time

            with self.tokenizer.as_target_tokenizer():
                decoded_tokens = self.tokenizer.batch_decode(
                    generated_tokens.detach().cpu().tolist(),
                    skip_special_tokens=True,
                    clean_up_tokenization_spaces=True
                )
                
            translations.extend(self.ip.postprocess_batch(decoded_tokens, lang=tgt_code))
            print(f"   ✅ [NMTTranslator] Batch {done_n}/{len(texts)} done ({gen_duration:.2f}s).", flush=True)

        return translations

    def unload(self):
        if self.model is not None:
            print(f"🧹 [NMTTranslator] Unloading Translation Engine to free RAM...", flush=True)
            del self.model
            del self.tokenizer
            del self.ip
            self.model = None
            self.tokenizer = None
            self.ip = None
            self.current_model_dir = None
            gc.collect()

def run_nmt(input_target: str, output_dir: str):
    project_root = Path(__file__).resolve().parent.parent.parent
    models_root = project_root / "models"

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
        print(f"❌ [NMTTranslator] Input file not found: {transcript_file}", flush=True)
        sys.exit(1)

    curr = os.path.abspath(transcript_file)
    job_root = None
    while curr and os.path.dirname(curr) != curr:
        if os.path.basename(curr).startswith("JOB-"):
            job_root = curr
            break
        curr = os.path.dirname(curr)

    target_language = "en"
    configured_source_lang = None
    if job_root:
        config_path = os.path.join(job_root, "job_config.json")
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as cfg:
                job_cfg = json.load(cfg)
                target_language = job_cfg.get("target_language", "en")
                configured_source_lang = job_cfg.get("source_language")

    load_domain_dictionary()

    with open(transcript_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    timeline = data.get("timeline", [])
    source_language = configured_source_lang or data.get("language", "mr")
    
    print(f"🔍 [NMTTranslator] Source: {source_language.upper()} ➔ Target: {target_language.upper()}", flush=True)
    engine = OfflineIndicTransEngine(models_root)

    # 1. Clean and normalize all segment texts individually
    input_texts = []
    for seg in timeline:
        raw_t = seg.get("text", "")
        cleaned_t = clean_asr_text(sanitize_nmt_input(raw_t, source_language)) if source_language.lower() in ["mr", "hi"] else raw_t.strip()
        input_texts.append(cleaned_t if cleaned_t else raw_t.strip())

    if input_texts:
        print(f"🧠 [NMTTranslator] Translating {len(input_texts)} discrete syntactic timeline segments...", flush=True)
        translated_segments = engine.translate_batch(input_texts, source_language, target_language)

        if target_language.lower() == "en":
            translated_segments = [smooth_natural_english(t) for t in translated_segments]
        elif target_language.lower() == "hi":
            translated_segments = [smooth_natural_hindi(t) for t in translated_segments]
        elif target_language.lower() == "mr":
            translated_segments = [smooth_natural_marathi(t) for t in translated_segments]

        for seg, trans_t in zip(timeline, translated_segments):
            seg["translated_text"] = trans_t

    engine.unload()

    os.makedirs(output_dir, exist_ok=True)
    json_out = os.path.join(output_dir, "master_translated.json")
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

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

    candidate_terms = []
    seen_words = set()
    all_text = " ".join([seg.get("text", "") for seg in timeline])
    
    devanagari_words = re.findall(r"[\u0900-\u097F]{4,}", all_text)
    for word in devanagari_words:
        w_clean = word.strip()
        if w_clean and w_clean not in seen_words and w_clean not in ASR_CORRECTIONS:
            seen_words.add(w_clean)
            candidate_terms.append({
                "original": w_clean,
                "replacement": w_clean,
                "type": "asr_corrections",
                "count": devanagari_words.count(w_clean)
            })

    candidate_file = os.path.join(output_dir, "candidate_terms.json")
    with open(candidate_file, "w", encoding="utf-8") as f:
        json.dump(candidate_terms[:15], f, indent=2, ensure_ascii=False)

    print(f"✅ [NMTTranslator] IndicTrans2 execution complete! Saved output to {output_dir}", flush=True)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        run_nmt(sys.argv[1], sys.argv[2])
    else:
        print("❌ [NMTTranslator] Missing required arguments: input_path output_dir", flush=True)
        sys.exit(1)
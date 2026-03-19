"""
validation_engine.py - Core validation logic with PDF handling and preprocessing

This module adds:
- PDF -> image conversion (multi-page support) using pdf2image
- Page selection heuristic (highest content + edge density)
- Image normalization pipeline before feature extraction
- Defensive handling when pdf2image/poppler isn't available
"""

import os
import tempfile
import logging
from pathlib import Path
from typing import Dict, Tuple, Optional, List
import base64
import io
import re
import unicodedata
import difflib

import cv2
import numpy as np

logger = logging.getLogger(__name__)

# Celebrity detection (optional - graceful failure if not available)
try:
    from celebrity_detection import CelebrityDetector
    _CELEBRITY_DETECTION_AVAILABLE = True
except ImportError:
    CelebrityDetector = None
    _CELEBRITY_DETECTION_AVAILABLE = False
    logger.info("Celebrity detection module not available")

# Configuration
WHITE_THRESHOLD = 240
BLANK_PERCENTAGE = 95
CONFIDENCE_THRESHOLD = 0.60
OUTLIER_THRESHOLD = -0.65
PDF_DPI = 300  # DPI for PDF conversion
MAX_DIMENSION = 1024  # resize so largest side <= MAX_DIMENSION

# OCR-based validation configuration
NAME_MATCH_THRESHOLD = 0.70  # Lowered from 0.80 to allow more variations
ID_PATTERN_CONFIDENCE_THRESHOLD = 0.6
ID_PATTERN_MIN_MATCHES = 2
LETTER_OCR_CONFIDENCE_BOOST_THRESHOLD = 0.70

# ID detection patterns and keywords
ID_KEYWORDS = [
    "aadhaar", "uid", "pan", "passport", "driving license", "driving licence",
    "government", "india", "valid", "issued", "authorized", "authorised",
    "name", "dob", "date of birth", "father", "address", "photo"
]

ID_NUMERIC_PATTERNS = [
    r'\b\d{12}\b',                      # Aadhaar pattern
    r'[A-Z]{5}\d{4}[A-Z]',              # PAN pattern
    r'\b\d{1,2}[-/]\d{1,2}[-/]\d{4}\b', # Date pattern
]

LETTER_KEYWORDS = [
    "training", "nominated", "service", "lbsnaa",
    "nomination", "letter", "program", "programme"
]

try:
    from pdf2image import convert_from_path
    _PDF2IMAGE_AVAILABLE = True
except Exception:
    convert_from_path = None
    _PDF2IMAGE_AVAILABLE = False

try:
    import pytesseract
    _PYTESSERACT_AVAILABLE = True
except Exception:
    pytesseract = None
    _PYTESSERACT_AVAILABLE = False

# Configure Tesseract path for Windows
if _PYTESSERACT_AVAILABLE:
    import platform
    import subprocess

    # Check if tesseract is in PATH, if not use common Windows location
    try:
        subprocess.run(['tesseract', '--version'], capture_output=True, check=True)
        logger.info("Tesseract found in system PATH")
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Tesseract not in PATH, try common Windows installation paths
        if platform.system() == 'Windows':
            possible_paths = [
                r'C:\Program Files\Tesseract-OCR\tesseract.exe',
                r'C:\Program Files (x86)\Tesseract-OCR\tesseract.exe',
                r'C:\Users\HP\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    pytesseract.pytesseract.tesseract_cmd = path
                    logger.info(f"Configured Tesseract path: {path}")
                    break
            else:
                logger.error("Tesseract not found in common locations. Please install from: https://github.com/UB-Mannheim/tesseract/wiki")
                _PYTESSERACT_AVAILABLE = False

# LLM Fallback Configuration
USE_LLM_FALLBACK = True  # Set to False to disable LLM fallback
LLM_FALLBACK_CONFIDENCE_THRESHOLD = 50.0  # Use LLM if OCR confidence < 50%
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')  # Set via environment variable

try:
    from openai import OpenAI
    _OPENAI_AVAILABLE = OPENAI_API_KEY is not None
    if _OPENAI_AVAILABLE:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        logger.info("OpenAI client initialized successfully")
    else:
        openai_client = None
        logger.warning("OpenAI API key not found. LLM fallback disabled.")
except ImportError:
    _OPENAI_AVAILABLE = False
    openai_client = None
    logger.warning("OpenAI library not installed. LLM fallback disabled.")


def _generate_name_variants(full_name: str) -> list:
    """
    Generate plausible name variants from a full name.

    For "Jack Daniel", generates variants like:
    - "jack daniel", "daniel jack" (original + reversed)
    - "j daniel", "jack d" (initial variants)
    - "jack", "daniel" (individual parts)
    """
    parts = full_name.lower().split()
    if not parts:
        return []

    variants = set()

    # Original
    variants.add(' '.join(parts))

    if len(parts) == 1:
        variants.add(parts[0])
        return list(variants)

    # Reversed order
    variants.add(' '.join(reversed(parts)))

    if len(parts) == 2:
        first, last = parts
        variants.add(f"{first[0]} {last}")        # J Daniel
        variants.add(f"{first} {last[0]}")         # Jack D
        variants.add(f"{first[0]} {last[0]}")      # J D
        variants.add(f"{first[0]}.{last}")         # J.Daniel
        variants.add(f"{first[0]}. {last}")        # J. Daniel
        variants.add(f"{first} {last[0]}.")        # Jack D.
        variants.add(first)                        # Jack
        variants.add(last)                         # Daniel

    elif len(parts) == 3:
        first, middle, last = parts
        variants.add(f"{first} {last}")                    # First Last (skip middle)
        variants.add(f"{first[0]} {last}")                 # F Last
        variants.add(f"{first} {middle[0]} {last}")        # First M Last
        variants.add(f"{first[0]} {middle[0]} {last}")     # F M Last
        variants.add(f"{first[0]}.{middle[0]}. {last}")    # F.M. Last
        variants.add(f"{first} {last[0]}")                 # First L
        variants.add(f"{first[0]} {middle} {last}")        # F Middle Last
        variants.add(first)
        variants.add(last)

    elif len(parts) >= 4:
        first = parts[0]
        last = parts[-1]
        variants.add(f"{first} {last}")
        variants.add(f"{first[0]} {last}")
        variants.add(f"{first} {last[0]}")
        variants.add(first)
        variants.add(last)

    variants.discard('')
    return list(variants)


def _extract_name_candidates_from_text(text: str, max_words: int = 4) -> list:
    """
    Extract candidate name sequences (n-grams) from OCR text.

    For text "Government of India J Daniel Address...",
    generates candidates like:
    ["government", "of", "india", "j", "daniel", "address",
     "government of", "of india", "india j", "j daniel", ...]
    """
    words = text.lower().split()
    # Cap at 1000 words for performance
    words = words[:1000]
    candidates = []

    # Single words
    candidates.extend(words)

    # Multi-word n-grams (2 to max_words)
    for n in range(2, min(max_words + 1, len(words) + 1)):
        for i in range(len(words) - n + 1):
            candidates.append(' '.join(words[i:i + n]))

    return candidates


def extract_name_from_text(text: str, user_name: str, document_type: str = "document") -> Dict[str, str]:
    """Extract and match name from OCR text with enhanced variant matching."""
    logger.info(f"[DEBUG] extract_name_from_text called: user_name='{user_name}', document_type='{document_type}', text_length={len(text) if text else 0}")

    if not text or not user_name:
        logger.info(f"[DEBUG] Returning NO_DATA: text={'empty' if not text else 'present'}, user_name={'empty' if not user_name else 'present'}")
        return {"match_status": "NO_DATA", "message": "No text or name provided"}

    # Map document types to user-friendly names for display messages
    doc_type_names = {
        "ID": "ID",
        "LETTER": "Document",
        "PHOTO": "photo"
    }
    doc_type_name = doc_type_names.get(document_type, "document")

    # Normalize both text and name for better matching
    normalized_text = normalize_ocr_text_for_names(text)
    normalized_name = normalize_ocr_text_for_names(user_name)

    text_lower = normalized_text.lower()
    name_lower = normalized_name.lower()
    name_words = name_lower.split()
    significant_words = [w for w in name_words if len(w) > 2]

    # === TIER 1: Exact full-name substring match ===
    if name_lower in text_lower:
        result = {"match_status": "MATCH", "message": f"Name verified in {doc_type_name}"}
        logger.info(f"[DEBUG] Exact match found! Returning: {result}")
        return result

    # === TIER 2: All significant words present (order-independent) ===
    if len(significant_words) > 1 and all(word in text_lower for word in significant_words):
        result = {"match_status": "MATCH", "message": f"Name verified in {doc_type_name}"}
        logger.info(f"[DEBUG] Word-level match found! Returning: {result}")
        return result

    # === TIER 3: Variant matching (initials, abbreviations, reversed order) ===
    name_variants = _generate_name_variants(normalized_name)
    text_candidates = _extract_name_candidates_from_text(normalized_text,
                                                         max_words=len(name_words) + 1)

    # Check if any multi-word variant appears in text
    multi_word_variants = [v for v in name_variants if ' ' in v and len(v.split()) >= 2]
    for variant in multi_word_variants:
        if variant in text_lower or variant in text_candidates:
            result = {"match_status": "MATCH", "message": f"Name verified in {doc_type_name}"}
            logger.info(f"[DEBUG] Variant match: '{variant}'. Returning: {result}")
            return result

    # === TIER 4: Fuzzy matching on variants and individual words ===
    text_words = text_lower.split()

    # Fuzzy match multi-word variants against text n-gram candidates
    for variant in multi_word_variants:
        matches = difflib.get_close_matches(variant, text_candidates, n=1,
                                             cutoff=NAME_MATCH_THRESHOLD)
        if matches:
            result = {"match_status": "MATCH", "message": f"Name verified in {doc_type_name}"}
            logger.info(f"[DEBUG] Fuzzy variant match: '{variant}' ~ '{matches[0]}'. Returning: {result}")
            return result

    # Fuzzy match individual significant words
    if len(significant_words) > 1:
        matched_count = 0
        for word in significant_words:
            word_matches = difflib.get_close_matches(word, text_words, n=1,
                                                      cutoff=NAME_MATCH_THRESHOLD)
            if word_matches:
                matched_count += 1

        match_ratio = matched_count / len(significant_words)
        if match_ratio >= 1.0:
            return {"match_status": "MATCH", "message": f"Name verified in {doc_type_name}"}
        elif match_ratio >= 0.5:
            return {"match_status": "PARTIAL", "message": f"Name partially verified in {doc_type_name}"}

    # === TIER 5: Single significant name part found (>3 chars to reduce false positives) ===
    single_name_variants = [v for v in name_variants if ' ' not in v and len(v) > 3]
    for single_variant in single_name_variants:
        if single_variant in text_words:
            return {"match_status": "PARTIAL", "message": f"Name partially verified in {doc_type_name}"}

    result = {"match_status": "NO_MATCH", "message": f"Name not found in {doc_type_name}"}
    logger.info(f"[DEBUG] No match found. Returning: {result}")
    return result


def normalize_ocr_text_for_names(text: str) -> str:
    """
    Normalize OCR text for better name matching.
    Handles common OCR errors and normalizes formatting.
    """
    if not text:
        return ""

    # Normalize unicode (handle diacritics)
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('ascii')

    # Common OCR error corrections
    replacements = {
        '0': 'O',  # Zero to letter O
        '1': 'I',  # One to letter I
        '|': 'I',  # Pipe to letter I
    }

    # Only replace digits when surrounded by letters (likely name context)
    for old, new in replacements.items():
        # Replace digit if preceded or followed by a letter
        text = re.sub(r'(?<=[A-Za-z])' + re.escape(old) + r'(?=[A-Za-z])', new, text)

    # Normalize line breaks to spaces (names can be split across OCR lines)
    text = text.replace('\n', ' ').replace('\r', ' ')

    # Normalize dots after single letters (initials like "J." -> "J")
    text = re.sub(r'\b([A-Za-z])\.\s*', r'\1 ', text)

    # Normalize whitespace
    text = ' '.join(text.split())

    return text


def detect_id_patterns(ocr_text: str) -> Dict[str, any]:
    """
    Detect ID-like patterns in OCR text.

    Returns:
        Dict with:
            - is_id_like: bool
            - confidence: float (0.0 to 1.0)
            - patterns_found: List[str]
            - details: Dict with pattern-specific info
    """
    if not ocr_text:
        return {
            "is_id_like": False,
            "confidence": 0.0,
            "patterns_found": [],
            "details": {}
        }

    text_upper = ocr_text.upper()
    text_lower = ocr_text.lower()
    patterns_found = []
    pattern_scores = []

    # Check for numeric patterns (Aadhaar, PAN, dates)
    for i, pattern in enumerate(ID_NUMERIC_PATTERNS):
        matches = re.findall(pattern, ocr_text, re.IGNORECASE)
        if matches:
            if i == 0:  # Aadhaar
                patterns_found.append("aadhaar_number")
                pattern_scores.append(0.4)
            elif i == 1:  # PAN
                patterns_found.append("pan_number")
                pattern_scores.append(0.4)
            elif i == 2:  # Date
                patterns_found.append("date_pattern")
                pattern_scores.append(0.2)

    # Check for ID-related keywords
    keyword_matches = []
    for keyword in ID_KEYWORDS:
        if keyword in text_lower:
            keyword_matches.append(keyword)

    if keyword_matches:
        patterns_found.append("id_keywords")
        # More keywords = higher confidence
        keyword_score = min(0.3, len(keyword_matches) * 0.05)
        pattern_scores.append(keyword_score)

    # Check for structured text (multiple distinct blocks)
    lines = [line.strip() for line in ocr_text.split('\n') if line.strip()]
    if len(lines) >= 5:
        patterns_found.append("structured_text")
        pattern_scores.append(0.2)

    # Calculate overall confidence
    total_score = sum(pattern_scores)
    confidence = min(1.0, total_score)

    # Determine if ID-like (need minimum number of pattern matches)
    is_id_like = len(patterns_found) >= ID_PATTERN_MIN_MATCHES and confidence > 0.3

    return {
        "is_id_like": is_id_like,
        "confidence": float(confidence),
        "patterns_found": patterns_found,
        "details": {
            "keyword_matches": keyword_matches[:5],  # Top 5 keywords
            "num_text_blocks": len(lines)
        }
    }


def detect_letter_keywords(ocr_text: str) -> Dict[str, any]:
    """
    Check if OCR text contains letter-specific keywords.

    Returns:
        Dict with:
            - has_keywords: bool
            - keywords_found: List[str]
            - match_count: int
    """
    if not ocr_text:
        return {
            "has_keywords": False,
            "keywords_found": [],
            "match_count": 0
        }

    text_lower = ocr_text.lower()
    keywords_found = []

    for keyword in LETTER_KEYWORDS:
        if keyword in text_lower:
            keywords_found.append(keyword)

    return {
        "has_keywords": len(keywords_found) > 0,
        "keywords_found": keywords_found,
        "match_count": len(keywords_found)
    }


def _is_pdf(path: str) -> bool:
    return str(path).lower().endswith(".pdf")


def _resize_image_keep_aspect(img: np.ndarray, max_dim: int = MAX_DIMENSION) -> np.ndarray:
    h, w = img.shape[:2]
    if max(h, w) <= max_dim:
        return img
    if h >= w:
        new_h = max_dim
        new_w = int(w * (max_dim / h))
    else:
        new_w = max_dim
        new_h = int(h * (max_dim / w))
    return cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_AREA)


def _preprocess_image(img: np.ndarray) -> np.ndarray:
    # Ensure color ordering and convert to BGR if single channel
    if img is None:
        return img

    # Convert to BGR if image is RGBA
    if img.shape[2] == 4:
        img = cv2.cvtColor(img, cv2.COLOR_BGRA2BGR)

    # Resize to stable max dimension
    img = _resize_image_keep_aspect(img, MAX_DIMENSION)

    # Denoise
    img = cv2.fastNlMeansDenoisingColored(img, None, h=10, hColor=10, templateWindowSize=7, searchWindowSize=21)

    # Convert to LAB and apply CLAHE on L channel for contrast normalization
    lab = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    l = clahe.apply(l)
    lab = cv2.merge((l, a, b))
    img = cv2.cvtColor(lab, cv2.COLOR_LAB2BGR)

    return img


def _compute_content_and_edge_scores(img: np.ndarray) -> Tuple[float, float]:
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # Adaptive threshold for robust foreground separation
    try:
        th = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 51, 9)
        content_pixels = np.sum(th > 0)
        total = gray.size
        content_density = (content_pixels / total) * 100
    except Exception:
        # fallback to simple threshold
        non_white = np.sum(gray < WHITE_THRESHOLD)
        total = gray.size
        content_density = (non_white / total) * 100

    edges = cv2.Canny(gray, 50, 150)
    edge_density = (np.sum(edges > 0) / gray.size) * 100

    return float(content_density), float(edge_density)


def extract_text_from_image(img: np.ndarray) -> Tuple[str, float]:
    """
    Extract text from image using OCR with confidence scores.

    Returns:
        tuple: (extracted_text, confidence_score)
    """
    if not _PYTESSERACT_AVAILABLE:
        logger.warning("pytesseract not available, skipping OCR")
        return "", 0.0

    try:
        # Convert to grayscale for OCR
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # Apply threshold to get better contrast
        _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

        # Extract text
        text = pytesseract.image_to_string(thresh, lang='eng')

        # Get confidence scores
        try:
            data = pytesseract.image_to_data(thresh, output_type=pytesseract.Output.DICT)
            confidences = [int(conf) for conf in data['conf'] if conf != '-1']
            avg_confidence = np.mean(confidences) if confidences else 0.0
        except Exception:
            avg_confidence = 0.0

        logger.info(f"OCR extracted {len(text)} characters with {avg_confidence:.1f}% confidence")
        return text.strip(), float(avg_confidence)

    except pytesseract.TesseractNotFoundError:
        logger.error("Tesseract executable not found. Install from: https://github.com/UB-Mannheim/tesseract/wiki")
        return "", 0.0
    except Exception as e:
        logger.error(f"OCR failed: {e}")
        return "", 0.0


def extract_text_with_llm(img: np.ndarray) -> Tuple[str, float]:
    """
    Extract text using OpenAI Vision API as fallback.

    Returns:
        tuple: (extracted_text, confidence_score)
    """
    if not _OPENAI_AVAILABLE:
        logger.warning("OpenAI not available, cannot use LLM fallback")
        return "", 0.0

    try:
        # Encode image to base64
        _, buffer = cv2.imencode('.png', img)
        img_base64 = base64.b64encode(buffer).decode('utf-8')

        # Call GPT-4o-mini Vision
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",  # Cheaper and faster than gpt-4-vision-preview
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Extract ALL text from this document image. Return ONLY the extracted text, preserving line breaks and structure. If it's an ID card, extract name, ID number, dates, and all visible text."
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{img_base64}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=1000
        )

        text = response.choices[0].message.content.strip()
        logger.info(f"LLM extracted {len(text)} characters")
        return text, 95.0  # LLM confidence is typically high

    except Exception as e:
        logger.error(f"LLM extraction failed: {e}")
        return "", 0.0


def extract_text_from_image_with_fallback(img: np.ndarray) -> Tuple[str, float, str]:
    """
    Extract text with OCR first, fallback to LLM if confidence is low.

    Returns:
        tuple: (text, confidence, method)
        method: "OCR", "LLM", or "FAILED"
    """
    # Try OCR first
    ocr_text, ocr_conf = extract_text_from_image(img)

    # If OCR succeeded with good confidence, use it
    if ocr_text and ocr_conf >= LLM_FALLBACK_CONFIDENCE_THRESHOLD:
        return ocr_text, ocr_conf, "OCR"

    # If OCR failed or low confidence, try LLM fallback
    if USE_LLM_FALLBACK and _OPENAI_AVAILABLE:
        logger.info(f"OCR confidence {ocr_conf:.1f}% below threshold, trying LLM fallback")
        llm_text, llm_conf = extract_text_with_llm(img)

        if llm_text:
            return llm_text, llm_conf, "LLM"

    # Return OCR result even if low confidence (better than nothing)
    return ocr_text, ocr_conf, "OCR" if ocr_text else "FAILED"


def _convert_pdf_to_images(pdf_path: str, dpi: int = PDF_DPI) -> List[str]:
    """Convert PDF to PNG files in a temp directory and return list of file paths.

    Raises informative errors if pdf2image/poppler not available.
    """
    if not _PDF2IMAGE_AVAILABLE:
        raise RuntimeError("pdf2image is not installed or could not be imported. Install pdf2image and ensure poppler is available.")

    temp_dir = tempfile.mkdtemp(prefix="pdf_pages_")
    try:
        pil_images = convert_from_path(pdf_path, dpi=dpi, fmt="png")
    except Exception as e:
        # Cleanup and re-raise with context
        try:
            os.rmdir(temp_dir)
        except Exception:
            pass
        raise RuntimeError(f"Failed to convert PDF to images: {e}")

    out_paths = []
    for i, pil_img in enumerate(pil_images):
        out_path = os.path.join(temp_dir, f"page_{i + 1}.png")
        pil_img.save(out_path, "PNG")
        out_paths.append(out_path)
    return out_paths


def extract_features_from_array(img: np.ndarray) -> Optional[Dict]:
    """Extract features from a preprocessed image array."""
    try:
        h, w = img.shape[:2]
        aspect_ratio = w / h if h > 0 else 0.0

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        non_white_pixels = np.sum(gray < WHITE_THRESHOLD)
        total_pixels = gray.size
        content_density = (non_white_pixels / total_pixels) * 100

        edges = cv2.Canny(gray, 50, 150)
        edge_pixels = np.sum(edges > 0)
        edge_density = (edge_pixels / total_pixels) * 100

        return {
            "aspect_ratio": float(aspect_ratio),
            "content_density": float(content_density),
            "edge_density": float(edge_density),
        }
    except Exception as e:
        logger.error(f"Error in extract_features_from_array: {e}")
        return None


def is_blank_document(image_path: str) -> Tuple[bool, float]:
    try:
        img = cv2.imread(str(image_path))
        if img is None:
            return False, 0.0
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        white_pixels = np.sum(gray >= WHITE_THRESHOLD)
        total_pixels = gray.size
        white_percentage = (white_pixels / total_pixels) * 100
        return (white_percentage >= BLANK_PERCENTAGE), float(white_percentage)
    except Exception as e:
        logger.error(f"Error checking blank document {image_path}: {e}")
        return False, 0.0


def _select_best_page(image_paths: List[str]) -> str:
    best_score = -1.0
    best_path = image_paths[0]
    for p in image_paths:
        img = cv2.imread(p)
        if img is None:
            continue
        img = _preprocess_image(img)
        content, edge = _compute_content_and_edge_scores(img)
        score = content + edge
        if score > best_score:
            best_score = score
            best_path = p
    return best_path


def validate_document(
    image_path: str,
    expected_type: str,
    ml_model,
    outlier_model,
    user_name: str = ""
) -> Dict:
    """Validation pipeline that accepts image path or PDF path.

    If `image_path` is a PDF, pages are converted and the most contentful
    page is selected for validation.
    """
    result = {
        "is_valid": False,
        "result": "ERROR",
        "expected_type": expected_type,
        "actual_type": None,
        "confidence": 0.0,
        "outlier_score": None,
        "message": "Processing failed",
        "features": None,
        "ocr_text": "",
        "base64_image": "",
        "name_match": {"match_status": "NO_DATA", "message": "Name matching not performed"},
        "ocr_status": "UNAVAILABLE",
        "ocr_confidence": 0.0,
        "extraction_method": "NONE",
        "ocr_override": False,
        "ocr_boost": False,
        "keywords_found": [],
        "override_reason": "",
        "id_patterns_detected": None,
        "celebrity_warning": None
    }

    temp_generated_paths: List[str] = []

    try:
        path_obj = Path(image_path)

        # If PDF -> convert to images and choose best page
        if _is_pdf(str(path_obj)):
            if not _PDF2IMAGE_AVAILABLE:
                result["result"] = "ERROR"
                result["message"] = "Server not configured for PDF processing (pdf2image/poppler missing)."
                logger.error("PDF upload but pdf2image not available")
                return result

            try:
                pages = _convert_pdf_to_images(str(path_obj), dpi=PDF_DPI)
                temp_generated_paths.extend(pages)
            except Exception as e:
                result["result"] = "ERROR"
                result["message"] = f"Failed to convert PDF to images: {e}"
                logger.error(result["message"])
                return result

            if not pages:
                result["result"] = "ERROR"
                result["message"] = "PDF conversion produced no pages"
                return result

            selected = _select_best_page(pages)
            image_to_use = selected
        else:
            image_to_use = str(path_obj)

        # Quick blank check on chosen image
        is_blank, white_pct = is_blank_document(image_to_use)
        if is_blank:
            result["result"] = "BLANK"
            result["message"] = (
                f"Document appears blank or mostly white ({white_pct:.1f}% white). Please upload a readable document."
            )
            logger.warning(f"Blank document detected: {image_to_use}")
            return result

        # Load image array and preprocess
        img = cv2.imread(image_to_use)
        if img is None:
            result["result"] = "ERROR"
            result["message"] = "Could not read image file after conversion/read."
            logger.error(f"Could not read image: {image_to_use}")
            return result

        preprocessed = _preprocess_image(img)

        # Create base64 image for preview
        try:
            _, buffer = cv2.imencode('.png', preprocessed)
            result["base64_image"] = base64.b64encode(buffer).decode('utf-8')
        except Exception as e:
            logger.warning(f"Failed to create base64 image: {e}")
            result["base64_image"] = ""

        features = extract_features_from_array(preprocessed)
        if features is None:
            result["result"] = "ERROR"
            result["message"] = "Feature extraction failed"
            return result

        result["features"] = features

        # Extract OCR text only for ID and LETTER documents
        if expected_type in ['ID', 'LETTER']:
            try:
                # Use LLM fallback if enabled, otherwise just OCR
                if USE_LLM_FALLBACK and _OPENAI_AVAILABLE:
                    ocr_text, ocr_confidence, extraction_method = extract_text_from_image_with_fallback(preprocessed)
                    result["extraction_method"] = extraction_method
                else:
                    ocr_text, ocr_confidence = extract_text_from_image(preprocessed)
                    result["extraction_method"] = "OCR"

                result["ocr_text"] = ocr_text
                result["ocr_confidence"] = ocr_confidence

                if ocr_text and len(ocr_text.strip()) > 0:
                    if ocr_confidence < 30:
                        result["ocr_status"] = "LOW_CONFIDENCE"
                        logger.warning(f"OCR confidence low: {ocr_confidence:.1f}%")
                    else:
                        result["ocr_status"] = "SUCCESS"
                else:
                    result["ocr_status"] = "NO_TEXT_FOUND"
                    logger.warning("OCR extraction returned no text")

                # Perform name matching if name provided
                if user_name:
                    logger.info(f"[DEBUG] Calling extract_name_from_text with user_name='{user_name}', expected_type='{expected_type}'")
                    name_match = extract_name_from_text(ocr_text, user_name, expected_type)
                    logger.info(f"[DEBUG] extract_name_from_text returned: {name_match}")
                    result["name_match"] = name_match
                else:
                    logger.info(f"[DEBUG] Skipping name matching - no user_name provided")
            except Exception as e:
                logger.error(f"OCR extraction failed: {e}")
                result["ocr_text"] = ""
                result["ocr_confidence"] = 0.0
                result["extraction_method"] = "FAILED"
                result["ocr_status"] = "FAILED"
                result["name_match"] = {"match_status": "NO_DATA", "message": "OCR processing failed"}
        else:
            result["ocr_text"] = ""
            result["ocr_confidence"] = 0.0
            result["extraction_method"] = "NOT_APPLICABLE"
            result["ocr_status"] = "NOT_APPLICABLE"
            result["name_match"] = {"match_status": "NOT_APPLICABLE", "message": f"Name matching not performed for {expected_type} documents"}

        # ========== CELEBRITY DETECTION FOR PHOTO DOCUMENTS ==========
        # Runs ONLY for PHOTO type documents, after preprocessing but before ML classification
        if expected_type == 'PHOTO' and _CELEBRITY_DETECTION_AVAILABLE:
            try:
                # Convert BGR to RGB for InsightFace library
                rgb_image = cv2.cvtColor(preprocessed, cv2.COLOR_BGR2RGB)

                # Run celebrity detection
                celebrity_result = CelebrityDetector.detect_celebrity(rgb_image)

                if celebrity_result["detected"]:
                    result["celebrity_warning"] = {
                        "detected": True,
                        "celebrity_name": celebrity_result["celebrity_name"],
                        "celebrity_id": celebrity_result["celebrity_id"],
                        "confidence": celebrity_result["confidence"],
                        "similarity": celebrity_result["similarity"],
                        "message": celebrity_result["message"]
                    }
                    logger.warning(
                        f"CELEBRITY DETECTED in photo: {celebrity_result['celebrity_name']} "
                        f"(confidence: {celebrity_result['confidence']:.1%})"
                    )
                else:
                    result["celebrity_warning"] = {
                        "detected": False,
                        "message": celebrity_result["message"]
                    }
                    logger.info(f"Celebrity check passed: {celebrity_result['message']}")

            except Exception as e:
                logger.error(f"Celebrity detection error: {e}")
                result["celebrity_warning"] = {
                    "detected": False,
                    "error": str(e),
                    "message": "Celebrity detection failed"
                }
        # ========== END CELEBRITY DETECTION ==========

        feature_array = np.array([[features["aspect_ratio"], features["content_density"], features["edge_density"]]])

        # Outlier detection
        if outlier_model is not None:
            try:
                outlier_score = outlier_model.score_samples(feature_array)[0]
                result["outlier_score"] = float(outlier_score)
                if outlier_score < OUTLIER_THRESHOLD:
                    result["result"] = "WRONG"
                    result["message"] = (
                        "This document appears to be invalid or in wrong format (screenshot, graphic, corrupted file, etc.). Please upload an actual document."
                    )
                    logger.warning(f"Outlier detected: {image_to_use} (score: {outlier_score:.3f})")
                    return result
            except Exception as e:
                logger.warning(f"Outlier model scoring failed: {e}")

        # ML predict
        prediction = ml_model.predict(feature_array)[0]
        probabilities = ml_model.predict_proba(feature_array)[0]
        confidence = float(np.max(probabilities))

        result["actual_type"] = prediction
        result["confidence"] = confidence

        # OCR-ML Conflict Resolution: Check if we should override ML prediction
        # Case 1: ID misclassified as PHOTO - use OCR to detect ID patterns
        if expected_type == 'ID' and prediction == 'PHOTO' and result.get("ocr_text") and result["ocr_status"] == "SUCCESS":
            id_patterns = detect_id_patterns(result["ocr_text"])
            result["id_patterns_detected"] = id_patterns

            if id_patterns["is_id_like"] and id_patterns["confidence"] > ID_PATTERN_CONFIDENCE_THRESHOLD:
                # Override ML prediction
                prediction = 'ID'
                result["actual_type"] = 'ID'
                result["ocr_override"] = True
                result["override_reason"] = f"OCR detected ID patterns: {', '.join(id_patterns['patterns_found'])}"
                # Boost confidence based on pattern detection
                confidence = max(confidence, id_patterns["confidence"])
                result["confidence"] = confidence
                logger.info(f"OCR override: ML predicted PHOTO, OCR detected ID patterns - accepting as ID")

        # Case 2: Letter with low ML confidence - check for keywords
        if expected_type == 'LETTER' and confidence < LETTER_OCR_CONFIDENCE_BOOST_THRESHOLD and result.get("ocr_text") and result["ocr_status"] == "SUCCESS":
            letter_check = detect_letter_keywords(result["ocr_text"])

            if letter_check["has_keywords"]:
                # Boost acceptance for letters with keywords
                result["ocr_boost"] = True
                result["keywords_found"] = letter_check["keywords_found"]
                logger.info(f"Letter keyword boost: found keywords {letter_check['keywords_found']} - boosting confidence")

                # Accept the letter despite low ML confidence
                result["is_valid"] = True
                result["result"] = "ACCEPT"
                result["message"] = f"✅ Letter validated via keyword detection: {', '.join(result['keywords_found'])} (ML confidence: {confidence:.1%})"
                logger.info(f"✓ Letter accepted via OCR keyword boost")
                return result

        if prediction != expected_type:
            result["result"] = "MISMATCH"
            result["message"] = (
                f"Wrong document type detected. You uploaded a {prediction} but we expected {expected_type}. (Confidence: {confidence:.1%})"
            )
            logger.warning(f"Type mismatch: expected {expected_type}, got {prediction} with {confidence:.1%} confidence")
            return result

        if confidence < CONFIDENCE_THRESHOLD:
            result["result"] = "SUSPICIOUS"
            result["message"] = (
                f"Document type is uncertain (confidence: {confidence:.1%}). Please upload a clearer or higher quality document."
            )
            logger.warning(f"Low confidence for {prediction}: {confidence:.1%} < {CONFIDENCE_THRESHOLD}")
            return result

        result["is_valid"] = True
        result["result"] = "ACCEPT"
        result["message"] = f"✅ Document validated successfully! ({confidence:.1%} confidence)"
        logger.info(f"✓ Document validated: {expected_type} with {confidence:.1%} confidence")
        return result

    except Exception as e:
        result["result"] = "ERROR"
        result["message"] = f"An error occurred during validation: {e}"
        logger.error(f"Validation error: {e}", exc_info=True)
        return result

    finally:
        # Cleanup temporary PDF-generated images
        for p in temp_generated_paths:
            try:
                os.remove(p)
            except Exception:
                pass
        # Remove temp dir if empty
        try:
            if temp_generated_paths:
                temp_dir = Path(temp_generated_paths[0]).parent
                if temp_dir.exists() and not any(temp_dir.iterdir()):
                    temp_dir.rmdir()
        except Exception:
            pass

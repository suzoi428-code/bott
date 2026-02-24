import os
import time
import requests
from typing import List, Dict, Optional, Tuple
import threading

BIN_INFO_CACHE: Dict[str, Dict] = {}
BIN_INFO_CACHE_LOCK = threading.Lock()

def get_bin_info(bin_number: str) -> Optional[Dict]:
    """
    Fetch BIN info from https://bins.antipublic.cc/bins/{bin_number}
    Returns dict with BIN info or None if not found
    """
    try:
        bin_6 = str(bin_number)[:6]
        if len(bin_6) < 6 or not bin_6.isdigit():
            return None
        
        with BIN_INFO_CACHE_LOCK:
            if bin_6 in BIN_INFO_CACHE:
                return BIN_INFO_CACHE[bin_6]
        
        response = requests.get(
            f"https://bins.antipublic.cc/bins/{bin_6}",
            timeout=5,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            with BIN_INFO_CACHE_LOCK:
                BIN_INFO_CACHE[bin_6] = data
                if len(BIN_INFO_CACHE) > 10000:
                    keys_to_remove = list(BIN_INFO_CACHE.keys())[:1000]
                    for key in keys_to_remove:
                        BIN_INFO_CACHE.pop(key, None)
            return data
        return None
    except Exception:
        return None

def format_bin_info(bin_info: Optional[Dict]) -> Tuple[str, str]:
    """Format BIN info for display. Returns (bin_display, country_display) tuple"""
    if not bin_info:
        return "", ""
    
    parts = []
    
    brand = bin_info.get("brand", "").upper()
    if brand:
        parts.append(brand)
    
    card_type = bin_info.get("type", "").upper()
    if card_type:
        parts.append(card_type)
    
    level = bin_info.get("level", "")
    if level:
        parts.append(level.upper())
    
    bank = bin_info.get("bank", "")
    if bank:
        parts.append(bank)
    
    country = bin_info.get("country_name", "") or bin_info.get("country", "")
    country_flag = bin_info.get("country_flag", "")
    country_display = ""
    if country:
        if country_flag:
            country_display = f"{country_flag} {country}"
        else:
            country_display = country
    
    return (" - ".join(parts) if parts else "", country_display)

UPLOADS_DIR = "uploads"

def ensure_uploads_dir():
    try:
        os.makedirs(UPLOADS_DIR, exist_ok=True)
    except Exception:
        pass

def _sanitize_filename_component(s: str) -> str:
    try:
        bad = '<>:"/\\|?*'
        s = "".join(c for c in str(s) if c not in bad and ord(c) >= 32)
        s = s.strip().replace("\n", " ").replace("\r", " ")
        s = "_".join(s.split())
        return s[:64] if len(s) > 64 else s
    except Exception:
        return "Unknown"

def _username_prefix_for_file(user) -> str:
    try:
        name = (getattr(user, "full_name", "") or "").strip()
    except Exception:
        name = ""
    if not name:
        try:
            uname = (getattr(user, "username", "") or "").strip()
        except Exception:
            uname = ""
        name = f"@{uname}" if uname else "Unknown⚡️"
    if name.startswith("@"):
        name = name[1:]
    return _sanitize_filename_component(name) or "Unknown"

def parse_cards_from_text(text: str) -> List[Dict]:
    """Optimized card parsing with fast path for well-formatted cards"""
    import neww as checkout
    import re
    
    cards = []
    seen_cards = set()  # Track card numbers we've already found
    text = text or ""
    
    fast_pattern = r'(\d{13,19})\s*\|\s*(\d{1,2})\s*\|\s*(\d{2,4})\s*\|\s*(\d{3,4})'
    fast_matches = re.finditer(fast_pattern, text)
    
    for match in fast_matches:
        try:
            number = match.group(1)
            month = int(match.group(2))
            year = int(match.group(3))
            cvv = match.group(4)
            
            if 1 <= month <= 12:
                if year < 100:
                    year += 2000
                
                card_key = f"{number}|{month}|{year}|{cvv}"
                if card_key not in seen_cards:
                    seen_cards.add(card_key)
                    cards.append({
                        'number': number,
                        'month': month,
                        'year': year,
                        'verification_value': cvv,
                        'name': 'Test Card'
                    })
        except (ValueError, IndexError):
            continue
    
    if len(cards) >= 10:
        return cards
    
    for line in text.splitlines():
        card = checkout.parse_cc_line(line)
        if card:
            card_key = f"{card['number']}|{card['month']}|{card['year']}|{card['verification_value']}"
            if card_key not in seen_cards:
                seen_cards.add(card_key)
                cards.append(card)
    
    if len(cards) < 5:  # If we found fewer than 5 cards, try aggressive extraction
        multi_line_cards = _extract_multiline_cards(text)
        for card in multi_line_cards:
            card_key = f"{card['number']}|{card['month']}|{card['year']}|{card['verification_value']}"
            if card_key not in seen_cards:
                seen_cards.add(card_key)
                cards.append(card)
    
    return cards

def _extract_multiline_cards(text: str) -> List[Dict]:
    """Extract card information that spans multiple lines with labels
    
    Can extract cards even when mixed with junk text like:
    4008950022280762dsfdsfewrwerewfew
    dsfdsf
    07sdfdsffds
    2029fdsf4wfwf753
    
    Extracts: 4008950022280762|07|2029|753
    """
    import re
    
    cards = []
    
    card_patterns = [
        r'(?:ccnum|cc num|card num|card number|card|cc|number|pan|cardnumber)\s*[:=\-]?\s*(\d{13,19})',
        r'\b(\d{13,19})\b',  # Standalone card number
        r'(\d{13,19})',  # Any sequence of 13-19 digits (even without word boundaries)
    ]
    
    exp_patterns = [
        r'(?:exp|expiry|expires|expiration|date|exp date|expiry date)\s*[:=\-]?\s*(\d{1,2})\s*[\/\-]\s*(\d{2,4})',
        r'(?:exp|expiry|expires|expiration|date|exp date|expiry date)\s*[:=\-]?\s*(\d{2})(\d{2,4})',  # MMYY or MMYYYY
        r'\b(\d{1,2})\s*[\/\-]\s*(\d{2,4})\b',  # Standalone MM/YY or MM/YYYY
        r'\b(\d{2})[^\d]*(\d{2,4})\b',  # 2 digits, junk, then 2-4 digits (aggressive)
    ]
    
    cvv_patterns = [
        r'(?:cvv|cvc|cv2|code|security code|security|pin)\s*[:=\-]?\s*(\d{3,4})',
        r'\b(\d{3,4})\b',  # Standalone 3-4 digit number (last resort)
    ]
    
    card_numbers = []
    seen_card_nums = {}  # Track unique card numbers with their first occurrence position
    for pattern in card_patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            card_num = match.group(1)
            if 13 <= len(card_num) <= 19:
                if card_num not in seen_card_nums:
                    seen_card_nums[card_num] = (match.start(), match.end())
                    card_numbers.append((card_num, match.start(), match.end()))
    
    for card_num, card_start, card_end in card_numbers:
        window_start = max(0, card_start - 200)
        window_end = min(len(text), card_end + 300)
        window_text = text[window_start:window_end]
        
        card_offset_in_window = card_start - window_start
        card_in_window_start = card_offset_in_window
        card_in_window_end = card_offset_in_window + len(card_num)
        
        exp_month = None
        exp_year = None
        year_match_pos = None  # Track year position to filter CVV later
        for pattern in exp_patterns:
            match = re.search(pattern, window_text, re.IGNORECASE)
            if match:
                if len(match.groups()) == 2:
                    exp_month = int(match.group(1))
                    year_str = match.group(2)
                    exp_year = int(year_str)
                    if exp_year < 100:
                        exp_year += 2000
                    if 1 <= exp_month <= 12:
                        break
                    else:
                        exp_month = None
                        exp_year = None
        
        if not exp_month or not exp_year:
            two_digit_matches = list(re.finditer(r'(\d{2})', window_text))
            year_matches = list(re.finditer(r'(\d{2,4})', window_text))
            
            for month_match in two_digit_matches:
                if (month_match.start() >= card_in_window_start and 
                    month_match.start() < card_in_window_end):
                    continue
                    
                potential_month = int(month_match.group(1))
                if 1 <= potential_month <= 12:  # Valid month
                    for year_match in year_matches:
                        if (year_match.start() >= card_in_window_start and 
                            year_match.start() < card_in_window_end):
                            continue
                            
                        if year_match.start() > month_match.end():
                            potential_year = int(year_match.group(1))
                            if potential_year >= 20 and potential_year <= 99:
                                potential_year += 2000
                            elif potential_year >= 2020 and potential_year <= 2099:
                                pass  # Already 4-digit
                            else:
                                continue
                            
                            if year_match.start() - month_match.end() <= 100:
                                exp_month = potential_month
                                exp_year = potential_year
                                year_match_pos = (year_match.start(), year_match.end())
                                break
                if exp_month and exp_year:
                    break
        
        cvv = None
        
        all_3digit = list(re.finditer(r'(\d{3})', window_text))
        all_4digit = list(re.finditer(r'(\d{4})', window_text))
        
        valid_3digit = []
        for match in all_3digit:
            if (match.start() >= card_in_window_start and 
                match.start() < card_in_window_end):
                continue
            
            if year_match_pos:
                year_start, year_end = year_match_pos
                if (match.start() >= year_start and match.start() < year_end):
                    continue
                
            potential = match.group(1)
            is_valid = True
            if exp_month and potential == str(exp_month).zfill(2):
                is_valid = False
            if exp_year and potential == str(exp_year)[-3:]:
                is_valid = False
            if is_valid:
                valid_3digit.append((match, potential))
        
        if len(valid_3digit) == 1:
            cvv = valid_3digit[0][1]
        else:
            for i, pattern in enumerate(cvv_patterns):
                matches = list(re.finditer(pattern, window_text, re.IGNORECASE))
                matches_after = [m for m in matches if m.start() > card_offset_in_window]
                matches_before = [m for m in matches if m.start() <= card_offset_in_window]
                sorted_matches = matches_after + matches_before
                
                for match in sorted_matches:
                    potential_cvv = match.group(1)
                    is_valid_cvv = (potential_cvv != card_num[-4:] and potential_cvv not in card_num)
                    if exp_year:
                        is_valid_cvv = is_valid_cvv and (potential_cvv != str(exp_year)[-4:] and 
                                                          potential_cvv != str(exp_year) and 
                                                          potential_cvv not in str(exp_year))
                    if exp_month:
                        is_valid_cvv = is_valid_cvv and (potential_cvv != str(exp_month).zfill(2))
                    
                    if is_valid_cvv:
                        if i == len(cvv_patterns) - 1:
                            if potential_cvv not in card_num:
                                cvv = potential_cvv
                                break
                        else:
                            cvv = potential_cvv
                            break
                if cvv:
                    break
        
        if exp_month and exp_year and cvv:
            cards.append({
                'number': card_num,
                'month': exp_month,
                'year': exp_year,
                'verification_value': cvv,
                'name': 'Test Card'
            })
    
    unique_cards = []
    seen = set()
    for card in cards:
        card_key = f"{card['number']}|{card['month']}|{card['year']}|{card['verification_value']}"
        if card_key not in seen:
            seen.add(card_key)
            unique_cards.append(card)
    
    return unique_cards

def parse_cards_from_file(file_path: str) -> List[Dict]:
    """Parse cards from file with optimizations for large files"""
    try:
        with open(file_path, "r", encoding="utf-8", buffering=8192) as f:
            content = f.read()
        return parse_cards_from_text(content)
    except Exception:
        return []

def progress_block(total: int, processed: int, approved: int, declined: int, charged: int, start_ts: float, captcha: int = 0) -> str:
    elapsed = time.time() - (start_ts or time.time())
    result = (
        f"🐢Total CC     : {total}\n"
        f"💬 Progress     : {processed}/{total}\n"
        f"✅  Approved    : {approved}\n"
        f"❌Declined     :  {declined}\n"
        f"💎 Charged     :  {charged}  \n"
    )
    if captcha > 0:
        result += f"⚠️  CAPTCHA     :  {captcha}\n"
    result += (
        f" Time Elapsed : {elapsed:.2f}s ⏱️\n"
        f"━━━━━━━━━━━━━\n"
        f"⌥ Dev: @LeVetche"
    )
    return result

def format_site_label(url: str) -> str:
    import neww as checkout
    try:
        return checkout.format_site_label(url)
    except Exception:
        return (url or "").strip()

def classify_prefix(code_display: str) -> str:
    import neww as checkout
    u = str(code_display or "").upper()
    if u == "SUCCESS":
        return "charged"
    if ('"ACTION_REQUIRED"' in u) or ('ACTION_REQUIRED' in u) or ('3D' in u):
        return "approved"
    if "CAPTCHA_REQUIRED" in u:
        return "unknown"
    approved_tokens = (
        "INCORRECT_CVC",
        "INVALID_CVC",
        "CVC",
        "CVV",
        "CSC",
        "SECURITY",
        "VERIFICATION",
        "PAYMENTS_CREDIT_CARD_CVV_INVALID",
        "PAYMENTS_CREDIT_CARD_VERIFICATION_VALUE_INVALID",
        "PAYMENTS_CREDIT_CARD_CSC_INVALID",
        "PAYMENTS_CREDIT_CARD_SECURITY_CODE_INVALID",
    )
    if any(tok in u for tok in approved_tokens):
        return "approved"
    if "HTTP_403" in u or "403" in u:
        return "unknown"
    if checkout.is_terminal_failure_code_display(code_display):
        return "declined"
    if checkout.is_unknown_code_display(code_display):
        return "unknown"
    if not code_display or not code_display.strip():
        return "approved"
    return "declined"

def result_notify_text(card: Dict, status: str, code_display: str, amount_display: Optional[str] = None, site_label: Optional[str] = None, user_info: Optional[str] = None, receipt_id: Optional[str] = None, user_id: Optional[int] = None) -> str:
    pan = str(card.get("number", "") or "")
    mm = int(card.get("month", 0) or 0)
    yy = int(card.get("year", 0) or 0)
    cvv = str(card.get("verification_value", "") or "")
    mm_str = f"{mm:02d}"
    yy_str = f"{yy % 100:02d}"
    
    if status == "charged":
        status_emoji = "💎"
        status_title = "CHARGED"
        status_color = "🟢"
    elif status == "approved":
        code_upper = (code_display or "").upper()
        is_3d = "3D" in code_upper or "ACTION_REQUIRED" in code_upper
        
        is_shopify = False
        if isinstance(site_label, str) and site_label.strip():
            site_lower = site_label.lower()
            is_shopify = "shopify" in site_lower or "myshopify" in site_lower
        
        if is_3d:
            status_emoji = "🔐"
            status_title = "APPROVED (3D)"
        elif is_shopify:
            status_emoji = "❎"
            status_title = "APPROVED (Shopify)"
        else:
            status_emoji = "✅"
            status_title = "APPROVED"
        status_color = "🟡"
    else:
        status_emoji = "❌"
        status_title = "DECLINED"
        status_color = "🔴"
    
    parts = [
        f"{status_color} <b>{status_title} {status_emoji}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"💳 <b>Card:</b> <code>{pan}|{mm_str}|{yy_str}|{cvv}</code>"
    ]
    
    bin_info = get_bin_info(pan)
    bin_display, country_display = format_bin_info(bin_info)
    if bin_display:
        parts.append(f"🏦 <b>BIN:</b> <code>{bin_display}</code>")
    if country_display:
        parts.append(f"🌍 <b>Country:</b> <code>{country_display}</code>")
    
    if status == "charged":
        parts.append('🔐 <b>Code:</b> <code>ProcessedReceipt</code>')
    else:
        code_upper = (code_display or "").upper()
        if "ACTION_REQUIRED" in code_upper and status == "approved":
            parts.append(f'🔐 <b>Code:</b> <code>{code_display}</code>')
        else:
            parts.append(f'🔐 <b>Code:</b> <code>{code_display}</code>')
    
    if isinstance(site_label, str) and site_label.strip():
        parts.append(f"🌐 <b>Site:</b> <code>{site_label.strip()}</code>")
    
    if isinstance(amount_display, str) and amount_display.strip():
        parts.append(f"💰 <b>Amount:</b> <code>{amount_display.strip()}</code>")
    
    if receipt_id and isinstance(receipt_id, str) and receipt_id.strip() and status in ("approved", "charged"):
        parts.append(f"🧾 <b>Receipt:</b> <code>{receipt_id.strip()}</code>")
    
    if isinstance(user_info, str) and user_info.strip():
        if user_id:
            user_link = f'<a href="tg://user?id={user_id}">{user_info.strip()}</a>'
            parts.append(f"👤 <b>User:</b> {user_link}")
        else:
            parts.append(f"👤 <b>User:</b> <code>{user_info.strip()}</code>")
    
    parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(parts)

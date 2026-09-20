import os
import logging
from typing import Dict, Any

from dotenv import load_dotenv
from groq import Groq
import base64
import json
import re
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote

import httpx

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ============================================================
# VAYRO TELEGRAM BOT
# ============================================================

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = (os.getenv("SUPABASE_SECRET_KEY", "").strip() or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip())

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError(
        "TELEGRAM_BOT_TOKEN is missing from .env"
    )



# ============================================================
# VAYRO CATALOG / STOCK DATABASE
# ============================================================

async def supabase_request(path: str, method: str = "GET", payload=None, params=None):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None, {"error": "Supabase environment variables are missing."}
    headers = {
        "apikey": SUPABASE_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if not SUPABASE_KEY.startswith("sb_"):
        headers["Authorization"] = f"Bearer {SUPABASE_KEY}"
    url = f"{SUPABASE_URL}/rest/v1/{path.lstrip('/')}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.request(method, url, headers=headers, json=payload, params=params)
        data = response.json() if response.content else None
        if not response.is_success:
            return response, data or {"error": response.text}
        return response, data
    except Exception as exc:
        logger.exception("Supabase request failed: %s", exc)
        return None, {"error": str(exc)}


async def fetch_products():
    columns = (
        "id,legacy_index,name,slug,category,type,description,price,"
        "compare_price,image_url,images,sizes,stock,active,colour"
    )
    response, data = await supabase_request(
        "products",
        params={"select": columns, "active": "eq.true", "order": "legacy_index.asc.nullslast,name.asc"},
    )
    if response is None or not response.is_success:
        logger.error("Could not fetch VAYRO products: %s", data)
        return []
    return data if isinstance(data, list) else []


def normalize_product(product):
    sizes = product.get("sizes") or []
    if isinstance(sizes, str):
        try:
            sizes = json.loads(sizes)
        except Exception:
            sizes = [x.strip() for x in re.split(r"[,\n]", sizes) if x.strip()]
    try:
        stock = max(0, int(float(product.get("stock") or 0)))
    except Exception:
        stock = 0
    return {
        "id": str(product.get("id") or ""),
        "name": str(product.get("name") or "").strip(),
        "price": product.get("price"),
        "colour": str(product.get("colour") or "").strip(),
        "stock": stock,
        "sizes": sizes if isinstance(sizes, list) else [],
        "slug": str(product.get("slug") or "").strip(),
        "active": product.get("active") is not False,
    }


def available_size_rows(product):
    rows = []
    for row in product.get("sizes", []):
        if not isinstance(row, dict):
            continue
        size = str(row.get("size", row.get("name", ""))).strip()
        quantity = row.get("quantity", row.get("qty", row.get("stock", 0)))
        try:
            quantity = max(0, int(float(quantity or 0)))
        except Exception:
            quantity = 0
        if size and quantity > 0:
            rows.append((size, quantity))
    return rows


def product_catalog_text(products):
    lines = []
    for raw in products:
        p = normalize_product(raw)
        if not p["name"]:
            continue
        size_rows = available_size_rows(p)
        size_text = ", ".join(f"{size} ({qty})" for size, qty in size_rows)
        if not size_text and p["stock"] > 0:
            size_text = "Stock available; size-wise quantity not supplied."
        if p["stock"] <= 0:
            size_text = "Out of stock"
        lines.append(
            f'ID: {p["id"]} | Product: {p["name"]} | Price: ₹{p["price"]} | '
            f'Colour: {p["colour"] or "Not specified"} | Total Stock: {p["stock"]} | Sizes: {size_text}'
        )
    return "\n".join(lines)


# The VAYRO storefront currently exposes sizes 6-10. These simple foot-length
# bands are kept in one place so they can be changed if VAYRO's official chart changes.
VAYRO_SIZE_RANGES = [
    (24.0, 24.7, "6"),
    (24.8, 25.5, "7"),
    (25.6, 26.3, "8"),
    (26.4, 27.1, "9"),
    (27.2, 28.0, "10"),
]


def recommend_vayro_size(foot_length: float):
    for low, high, size in VAYRO_SIZE_RANGES:
        if low <= foot_length <= high:
            return size
    if foot_length < VAYRO_SIZE_RANGES[0][0]:
        return "6"
    if foot_length > VAYRO_SIZE_RANGES[-1][1]:
        return "10"
    return None


async def save_restock_alert(user_id: int, chat_id: int, product: dict):
    payload = {
        "telegram_user_id": user_id,
        "telegram_chat_id": chat_id,
        "product_id": product["id"],
        "product_name": product["name"],
        "active": True,
    }
    response, data = await supabase_request(
        "telegram_restock_alerts",
        method="POST",
        payload=payload,
        params={"on_conflict": "telegram_user_id,product_id"},
    )
    if response is None or not response.is_success:
        logger.error("Could not save restock alert: %s", data)
        return False
    return True


async def poll_restock_alerts(application):
    while True:
        try:
            response, alerts = await supabase_request(
                "telegram_restock_alerts",
                params={"select": "id,telegram_user_id,telegram_chat_id,product_id,product_name", "active": "eq.true"},
            )
            if response is not None and response.is_success and isinstance(alerts, list):
                for alert in alerts:
                    product_id = str(alert.get("product_id") or "")
                    if not product_id:
                        continue
                    p_response, products = await supabase_request(
                        "products",
                        params={"select": "id,name,price,colour,stock,sizes,active", "id": f"eq.{quote(product_id, safe='')}", "limit": "1"},
                    )
                    if p_response is None or not p_response.is_success or not products:
                        continue
                    product = normalize_product(products[0])
                    if not product["active"] or product["stock"] <= 0:
                        continue
                    sizes = available_size_rows(product)
                    size_text = ", ".join(size for size, _ in sizes) or "Available"
                    try:
                        await application.bot.send_message(
                            chat_id=int(alert["telegram_chat_id"]),
                            text=(
                                "Restock Alert\n\n"
                                f'Product: {product["name"]}\n'
                                "Status: In Stock\n"
                                f"Sizes: {size_text}"
                            ),
                            reply_markup=main_menu(),
                        )
                        await supabase_request(
                            f'telegram_restock_alerts?id=eq.{quote(str(alert["id"]), safe="")}',
                            method="PATCH",
                            payload={"active": False, "notified_at": "now()"},
                        )
                    except Exception:
                        logger.exception("Restock notification failed for alert %s", alert.get("id"))
        except Exception:
            logger.exception("Restock poll failed.")
        await asyncio.sleep(60)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger("VAYRO")


# ============================================================
# GROQ AI
# ============================================================

GROQ_TEXT_MODEL = "qwen/qwen3.8-27b"
GROQ_VISION_MODEL = "qwen/qwen3.8-27b"

try:
    groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
    if groq_client:
        logger.info("Groq client initialized successfully.")
    else:
        logger.warning("GROQ_API_KEY is not configured. AI features will be unavailable.")
except Exception as e:
    logger.exception("Groq client initialization failed: %s", e)
    groq_client = None


# ============================================================
# USER SESSIONS
# ============================================================

sessions: Dict[int, Dict[str, Any]] = {}


def reset_session(user_id: int):

    sessions[user_id] = {
        "feature": None,
        "step": None,
        "data": {},
    }


def get_session(user_id: int):

    if user_id not in sessions:
        reset_session(user_id)

    return sessions[user_id]


# ============================================================
# MAIN MENU
# ============================================================

def main_menu():

    keyboard = [
        [
            InlineKeyboardButton(
                "Find My Sneaker",
                callback_data="feature_sneaker",
            ),
            InlineKeyboardButton(
                "Find My Size",
                callback_data="feature_size",
            ),
        ],
        [
            InlineKeyboardButton(
                "Match My Outfit",
                callback_data="feature_outfit",
            ),
            InlineKeyboardButton(
                "Restock Alert",
                callback_data="feature_alert",
            ),
        ],
        [
            InlineKeyboardButton(
                "Gift Finder",
                callback_data="feature_gift",
            ),
            InlineKeyboardButton(
                "Sneaker Care",
                callback_data="feature_care",
            ),
        ],
    ]

    return InlineKeyboardMarkup(keyboard)


def back_menu():

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Back to VAYRO",
                    callback_data="back_home",
                )
            ]
        ]
    )


# ============================================================
# WELCOME MESSAGE
# ============================================================

WELCOME_TEXT = (
    "Welcome to VAYRO.\n\n"
    "How can I help you today?\n\n"
    "Choose an option from below."
)


# ============================================================
# START
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    user = update.effective_user

    if user:
        reset_session(user.id)

    await update.message.reply_text(
        WELCOME_TEXT,
        reply_markup=main_menu(),
    )


# ============================================================
# GROQ TEXT
# ============================================================

async def ask_groq(prompt: str) -> str:
    if groq_client is None:
        return (
            "The VAYRO AI assistant is temporarily unavailable. "
            "Please try again shortly."
        )

    try:
        response = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model=GROQ_TEXT_MODEL,
            messages=[
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_completion_tokens=500,
            stream=False,
        )

        output_text = (
            response.choices[0].message.content or ""
        ).strip()

        if output_text:
            return output_text

        logger.error("Groq returned an empty response.")
        return (
            "I could not generate a response right now. "
            "Please try again."
        )

    except Exception as e:
        logger.exception("Groq text request failed: %s", e)
        return (
            "The VAYRO AI assistant is temporarily unavailable. "
            "Please try again shortly."
        )


# ============================================================
# FIND MY SNEAKER
# ============================================================

async def start_sneaker_finder(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query
    user_id = query.from_user.id

    reset_session(user_id)

    session = get_session(user_id)

    session["feature"] = "sneaker"
    session["step"] = "style"

    # Do NOT edit/delete the old message.
    await query.message.reply_text(
        "Find My Sneaker\n\n"
        "Let's find a sneaker that matches your style.\n\n"
        "What kind of sneaker are you looking for?\n\n"
        "For example:\n"
        "Daily wear\n"
        "Streetwear\n"
        "Minimal\n"
        "Sporty\n"
        "Casual\n"
        "Bold",
        reply_markup=back_menu(),
    )


async def handle_sneaker_finder(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    message: str,
):

    user_id = update.effective_user.id
    session = get_session(user_id)

    if session["step"] == "style":

        session["data"]["style"] = message
        session["step"] = "colour"

        await update.message.reply_text(
            "What colour or colour combination do you prefer?"
        )

        return

    if session["step"] == "colour":

        session["data"]["colour"] = message
        session["step"] = "budget"

        await update.message.reply_text(
            "What is your approximate budget in INR?"
        )

        return

    if session["step"] == "budget":

        session["data"]["budget"] = message
        style = session["data"].get("style", "")
        colour = session["data"].get("colour", "")
        budget = session["data"].get("budget", "")

        loading_message = await update.message.reply_text(
            "Find My Sneaker\n\nPlease wait a moment..."
        )

        products = await fetch_products()
        catalog = product_catalog_text(products)
        if not catalog:
            result = "Catalog data is currently unavailable."
        else:
            prompt = f"""
You are the VAYRO sneaker assistant.
Choose ONE product ONLY from the live VAYRO catalog below.
Never invent a product, price, colour, size or stock.

Customer:
Style: {style}
Colour: {colour}
Budget: {budget} INR

Live catalog:
{catalog}

Return ONLY:
Product: [exact catalog product name]
Price: [exact catalog price]
Colour: [exact catalog colour or Not specified]
Sizes: [only currently available sizes]
Style: [2-4 word style match]

Maximum 5 lines. No emojis.
If nothing reasonably matches the customer's budget/style/colour, return exactly:
No matching VAYRO sneaker found.
"""
            result = await ask_groq(prompt)

        reset_session(user_id)
        await loading_message.edit_text(
            "Find My Sneaker\n\n" + result,
            reply_markup=main_menu(),
        )


# ============================================================
# FIND MY SIZE
# ============================================================

async def start_size_finder(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query
    user_id = query.from_user.id

    reset_session(user_id)

    session = get_session(user_id)

    session["feature"] = "size"
    session["step"] = "length"

    await query.message.reply_text(
        "Find My Size\n\n"
        "Let's find the right size direction for you.\n\n"
        "Tell me your foot length in centimetres.\n\n"
        "Example: 26.5",
        reply_markup=back_menu(),
    )


async def handle_size_finder(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    message: str,
):

    user_id = update.effective_user.id
    session = get_session(user_id)

    if session["step"] != "length":
        return

    try:
        foot_length = float(message.replace(",", ".").strip().lower().replace("cm", "").strip())
    except ValueError:
        await update.message.reply_text("Please enter your foot length in cm. Example: 26.5")
        return

    if foot_length <= 0 or foot_length > 40:
        await update.message.reply_text("Please enter a valid foot length in cm. Example: 26.5")
        return

    size = recommend_vayro_size(foot_length)
    reset_session(user_id)
    await update.message.reply_text(
        "Find My Size\n\n"
        f"Foot Length: {foot_length:g} cm\n"
        f"Recommended Size: {size or 'Check size guide'}",
        reply_markup=main_menu(),
    )


# ============================================================
# MATCH MY OUTFIT
# ============================================================

async def start_outfit(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query
    user_id = query.from_user.id

    reset_session(user_id)

    session = get_session(user_id)

    session["feature"] = "outfit"
    session["step"] = "photo"

    await query.message.reply_text(
        "Match My Outfit\n\n"
        "Send me a clear photo of your outfit.\n\n"
        "I'll analyze the colours and overall style "
        "and suggest the type of VAYRO sneaker that "
        "could work with it.",
        reply_markup=back_menu(),
    )


async def handle_outfit_photo(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user_id = update.effective_user.id
    session = get_session(user_id)

    if session.get("feature") != "outfit":
        await update.message.reply_text(
            "Please choose Match My Outfit from the VAYRO menu.",
            reply_markup=main_menu(),
        )
        return

    loading_message = None

    try:
        photo = update.message.photo[-1]
        telegram_file = await context.bot.get_file(photo.file_id)
        image_bytes = await telegram_file.download_as_bytearray()

        # ----------------------------------------------------
        # GET LIVE VAYRO PRODUCTS FROM SUPABASE
        # ----------------------------------------------------
        products = await fetch_products()
        catalog = product_catalog_text(products)

        if not catalog:
            raise RuntimeError(
                "VAYRO product catalog is currently unavailable."
            )

        # ----------------------------------------------------
        # MATCH MY OUTFIT PROMPT
        # ----------------------------------------------------
        prompt = f"""
You are the VAYRO "Match My Outfit" AI assistant.

Analyze the uploaded outfit photo carefully.

Identify:
1. The main colour of the TOP.
2. The main colour of the BOTTOM.
3. Whether footwear is clearly visible.

FOOTWEAR RULE:
- If footwear is NOT clearly visible, DO NOT include a Sneaker line.
- If footwear IS clearly visible, include only the visible footwear colour
  in the Sneaker line.
- Never guess footwear that is hidden, cropped out, or not visible.

============================================================
IF NO FOOTWEAR IS VISIBLE
============================================================

Match My Outfit

Top: [top colour]
Bottom: [bottom colour]

VAYRO Suggestion:
1. [exact VAYRO product name]
2. [exact VAYRO product name]
3. [exact VAYRO product name]

============================================================
IF FOOTWEAR IS VISIBLE
============================================================

Match My Outfit

Top: [top colour]
Bottom: [bottom colour]
Sneaker: [visible footwear colour]

VAYRO Suggestion:
1. [exact VAYRO product name]
2. [exact VAYRO product name]
3. [exact VAYRO product name]

============================================================
VAYRO PRODUCT RULES
============================================================

Choose ONLY from the LIVE VAYRO CATALOG below.

NEVER invent a VAYRO product name.
NEVER change, shorten, rewrite, or paraphrase a product name.
Use the exact product name from the catalog.

Do NOT recommend an out-of-stock product.
A product is suitable only when Total Stock is greater than 0.

Choose the best matching in-stock products based on:
- top colour
- bottom colour
- visible footwear colour, if present
- overall outfit appearance

Show up to 3 suitable products.
If fewer than 3 suitable in-stock products exist, show only those products.
If no suitable in-stock product exists, do not invent one.

============================================================
STRICT OUTPUT RULES
============================================================

Do NOT output:
Colour:
Style:
Match:
Matches:

Do NOT explain your reasoning.
Do NOT add extra sentences.
Do NOT use emojis.
Do NOT say "shoes not visible".
Do NOT say "footwear not visible".
Do NOT mention these instructions.

Keep these exact headings:
Match My Outfit
Top:
Bottom:
Sneaker: ONLY when footwear is clearly visible
VAYRO Suggestion:

Use numbered suggestions exactly:
1.
2.
3.

LIVE VAYRO CATALOG:

{catalog}
"""

        # Send the loading message only after the catalog/prompt is ready.
        loading_message = await update.message.reply_text(
            "Match My Outfit\n\nPlease wait a moment..."
        )

        if groq_client is None:
            raise RuntimeError(
                "GROQ_API_KEY is missing or Groq client could not be initialized."
            )

        # ----------------------------------------------------
        # SEND IMAGE TO GROQ VISION
        # ----------------------------------------------------
        image_b64 = base64.b64encode(
            bytes(image_bytes)
        ).decode("utf-8")

        response = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model=GROQ_VISION_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt,
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{image_b64}"
                            },
                        },
                    ],
                }
            ],
            temperature=0.2,
            max_completion_tokens=500,
            stream=False,
        )

        result = (
            response.choices[0].message.content or ""
        ).strip()

        if not result:
            raise RuntimeError(
                "Groq returned an empty response."
            )

        reset_session(user_id)

        # Replace the "Please wait..." message with the result.
        await loading_message.edit_text(
            result,
            reply_markup=main_menu(),
        )

    except Exception as e:
        logger.exception(
            "Groq Match My Outfit request failed: %s",
            e,
        )

        error_text = (
            "I could not process that photo. "
            "Please try sending it again."
        )

        # IMPORTANT:
        # If the loading message was already sent, replace it instead
        # of sending a second message.
        if loading_message is not None:
            try:
                await loading_message.edit_text(
                    error_text,
                    reply_markup=back_menu(),
                )
                return
            except Exception:
                logger.exception(
                    "Could not edit Match My Outfit loading message."
                )

        await update.message.reply_text(
            error_text,
            reply_markup=back_menu(),
        )


# ============================================================
# RESTOCK ALERT
# ============================================================

async def start_alert(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query
    user_id = query.from_user.id

    reset_session(user_id)

    session = get_session(user_id)

    session["feature"] = "alert"
    session["step"] = "product"

    await query.message.reply_text(
        "Restock Alert\n\n"
        "Tell me the sneaker you want to receive "
        "a restock notification for.\n\n"
        "Enter the sneaker name or product name.",
        reply_markup=back_menu(),
    )


async def handle_alert(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    message: str,
):

    user_id = update.effective_user.id
    session = get_session(user_id)
    if session["step"] != "product":
        return

    product_query = message.strip()
    products = await fetch_products()
    if not products:
        reset_session(user_id)
        await update.message.reply_text("Restock Alert\n\nProduct data is currently unavailable.", reply_markup=main_menu())
        return

    normalized = [normalize_product(p) for p in products]
    exact = next((p for p in normalized if p["name"].casefold() == product_query.casefold()), None)
    if exact is None:
        exact = next((p for p in normalized if product_query.casefold() in p["name"].casefold()), None)

    if exact is None:
        reset_session(user_id)
        await update.message.reply_text(
            "Restock Alert\n\nProduct not found. Please enter the VAYRO product name.",
            reply_markup=main_menu(),
        )
        return

    if exact["stock"] > 0:
        sizes = available_size_rows(exact)
        size_text = ", ".join(size for size, _ in sizes) or "Available"
        reset_session(user_id)
        await update.message.reply_text(
            "Restock Alert\n\n"
            f'Product: {exact["name"]}\n'
            "Status: In Stock\n"
            f"Sizes: {size_text}",
            reply_markup=main_menu(),
        )
        return

    saved = await save_restock_alert(user_id, update.effective_chat.id, exact)
    reset_session(user_id)
    if saved:
        text = (
            "Restock Alert\n\n"
            f'Product: {exact["name"]}\n'
            "Status: Out of Stock\n"
            "Alert saved. We'll notify you when it is back in stock."
        )
    else:
        text = (
            "Restock Alert\n\n"
            f'Product: {exact["name"]}\n'
            "Status: Out of Stock\n"
            "We could not save the alert. Please try again."
        )
    await update.message.reply_text(text, reply_markup=main_menu())


# ============================================================
# GIFT FINDER
# ============================================================

async def start_gift(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query
    user_id = query.from_user.id

    reset_session(user_id)

    session = get_session(user_id)

    session["feature"] = "gift"
    session["step"] = "recipient"

    await query.message.reply_text(
        "Gift Finder\n\n"
        "Let's find a sneaker gift that fits their style.\n\n"
        "Who are you buying for?\n\n"
        "For example:\n"
        "Brother\n"
        "Sister\n"
        "Friend\n"
        "Partner\n"
        "Teenager",
        reply_markup=back_menu(),
    )


async def handle_gift_finder(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    message: str,
):

    user_id = update.effective_user.id
    session = get_session(user_id)

    if session["step"] == "recipient":

        session["data"]["recipient"] = message
        session["step"] = "budget"

        await update.message.reply_text(
            "What is your gift budget in INR?"
        )

        return

    if session["step"] == "budget":

        session["data"]["budget"] = message
        session["step"] = "style"

        await update.message.reply_text(
            "What kind of style do they usually like?\n\n"
            "Minimal\n"
            "Streetwear\n"
            "Sporty\n"
            "Classic\n"
            "Bold"
        )

        return

    if session["step"] == "style":

        session["data"]["style"] = message
        recipient = session["data"].get("recipient", "")
        budget = session["data"].get("budget", "")
        style = session["data"].get("style", "")

        loading_message = await update.message.reply_text(
            "Gift Finder\n\nPlease wait a moment..."
        )

        products = await fetch_products()
        catalog = product_catalog_text(products)
        if not catalog:
            result = "Catalog data is currently unavailable."
        else:
            prompt = f"""
You are the VAYRO Gift Finder.
Choose ONE product ONLY from the live VAYRO catalog below.
Never invent a product, price, colour or stock.

Recipient: {recipient}
Budget: {budget} INR
Style: {style}

Live catalog:
{catalog}

Return ONLY:
Product: [exact catalog product name]
Price: [exact catalog price]
Colour: [exact catalog colour or Not specified]
Best For: [one short reason]

Maximum 4 lines. No emojis.
If nothing reasonably matches, return exactly:
No matching VAYRO gift found.
"""
            result = await ask_groq(prompt)

        reset_session(user_id)
        await loading_message.edit_text(
            "Gift Finder\n\n" + result,
            reply_markup=main_menu(),
        )


# ============================================================
# SNEAKER CARE
# ============================================================

async def start_care(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query
    user_id = query.from_user.id

    reset_session(user_id)

    session = get_session(user_id)

    session["feature"] = "care"
    session["step"] = "question"

    await query.message.reply_text(
        "Sneaker Care\n\n"
        "How can I help you care for your sneakers?\n\n"
        "For example:\n"
        "How should I clean my sneakers?\n"
        "How do I remove a stain?\n"
        "How should I store them?\n"
        "How do I keep white sneakers clean?",
        reply_markup=back_menu(),
    )


async def handle_care(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    message: str,
):

    user_id = update.effective_user.id

    prompt = f"""
You are the VAYRO Sneaker Care assistant.

Customer question:

{message}

Give a concise answer.

Return ONLY:
Care: [one short tip]
Avoid: [one short warning]
Next: [one short action]

Maximum 3 lines. If material is unknown, advise checking product care instructions.

Do not use emojis.
"""

    loading_message = await update.message.reply_text(
        "Sneaker Care\n\nPlease wait a moment..."
    )

    result = await ask_groq(prompt)

    reset_session(user_id)

    await loading_message.edit_text(
        "Sneaker Care\n\n" + result,
        reply_markup=main_menu(),
    )


# ============================================================
# BUTTON HANDLER
# ============================================================

async def button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query

    await query.answer()

    data = query.data

    if data == "back_home":

        reset_session(query.from_user.id)

        await query.message.reply_text(
            WELCOME_TEXT,
            reply_markup=main_menu(),
        )

        return

    if data == "feature_sneaker":

        await start_sneaker_finder(
            update,
            context,
        )

        return

    if data == "feature_size":

        await start_size_finder(
            update,
            context,
        )

        return

    if data == "feature_outfit":

        await start_outfit(
            update,
            context,
        )

        return

    if data == "feature_alert":

        await start_alert(
            update,
            context,
        )

        return

    if data == "feature_gift":

        await start_gift(
            update,
            context,
        )

        return

    if data == "feature_care":

        await start_care(
            update,
            context,
        )

        return


# ============================================================
# TEXT HANDLER
# ============================================================

async def text_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not update.message:
        return

    user_id = update.effective_user.id

    session = get_session(user_id)

    message = update.message.text.strip()

    feature = session.get("feature")

    if not feature:

        await update.message.reply_text(
            WELCOME_TEXT,
            reply_markup=main_menu(),
        )

        return

    if feature == "sneaker":

        await handle_sneaker_finder(
            update,
            context,
            message,
        )

        return

    if feature == "size":

        await handle_size_finder(
            update,
            context,
            message,
        )

        return

    if feature == "alert":

        await handle_alert(
            update,
            context,
            message,
        )

        return

    if feature == "gift":

        await handle_gift_finder(
            update,
            context,
            message,
        )

        return

    if feature == "care":

        await handle_care(
            update,
            context,
            message,
        )

        return

    if feature == "outfit":

        await update.message.reply_text(
            "Please send an outfit photo.",
            reply_markup=back_menu(),
        )


# ============================================================
# PHOTO HANDLER
# ============================================================

async def photo_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    user_id = update.effective_user.id

    session = get_session(user_id)

    if session.get("feature") == "outfit":

        await handle_outfit_photo(
            update,
            context,
        )

        return

    await update.message.reply_text(
        "Please choose Match My Outfit first.",
        reply_markup=main_menu(),
    )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):

    logger.exception(
        "Unhandled bot error:",
        exc_info=context.error,
    )


# ============================================================
# RENDER HEALTH SERVER
# ============================================================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health" or self.path == "/":
            body = b"VAYRO Telegram Bot is running"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        # Keep Render logs focused on bot activity.
        return


def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    logger.info("Render health server listening on 0.0.0.0:%s", port)
    server.serve_forever()


# ============================================================
# MAIN
# ============================================================

def main():

    logger.info("----------------------------------------")
    logger.info("Starting VAYRO Telegram Bot")
    logger.info("----------------------------------------")

    # Render Web Services require an HTTP listener on 0.0.0.0:$PORT.
    # This lightweight health server runs alongside Telegram polling.
    threading.Thread(
        target=start_health_server,
        name="render-health-server",
        daemon=True,
    ).start()

    # Telegram connection: force IPv4 and allow a little more time/retry.
    # curl -4 works on this PC, while Python/httpx was timing out during
    # the initial getMe() request. HTTPX supports local_address on its
    # low-level transport, which lets us bind Telegram traffic to IPv4.
    telegram_request = HTTPXRequest(
        connection_pool_size=8,
        connect_timeout=20.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=10.0,
        http_version="1.1",
        httpx_kwargs={
            "transport": httpx.AsyncHTTPTransport(
                local_address="0.0.0.0",
                retries=2,
            )
        },
    )

    telegram_get_updates_request = HTTPXRequest(
        connection_pool_size=2,
        connect_timeout=20.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=10.0,
        http_version="1.1",
        httpx_kwargs={
            "transport": httpx.AsyncHTTPTransport(
                local_address="0.0.0.0",
                retries=2,
            )
        },
    )

    application = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .request(telegram_request)
        .get_updates_request(telegram_get_updates_request)
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            button_handler,
        )
    )

    application.add_handler(
        MessageHandler(
            filters.PHOTO,
            photo_handler,
        )
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_handler,
        )
    )

    application.add_error_handler(
        error_handler,
    )

    logger.info("VAYRO Telegram Bot is running.")
    logger.info("Telegram HTTP transport: IPv4, HTTP/1.1, connect timeout 20s, retries 2")
    logger.info("Groq text model: %s", GROQ_TEXT_MODEL)
    logger.info("Groq vision model: %s", GROQ_VISION_MODEL)

    # Start the background restock checker as a normal asyncio task.
    # Using asyncio.create_task here avoids creating an Application task
    # before the PTB Application has entered its running state.
    restock_task = None

    async def post_init(app):
        nonlocal restock_task
        restock_task = asyncio.create_task(
            poll_restock_alerts(app),
            name="vayro-restock-alert-poller",
        )
        app.bot_data["restock_task"] = restock_task

    async def post_shutdown(app):
        task = app.bot_data.pop("restock_task", None)
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    application.post_init = post_init
    application.post_shutdown = post_shutdown

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        close_loop=False,
    )


if __name__ == "__main__":
    main()
import asyncio
import logging
import os
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)

from database import Database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]

# Мінімальний термін в групі для голосування та отримання оцінок (днів)
MIN_DAYS_TO_VOTE = int(os.environ.get("MIN_DAYS_TO_VOTE", "180"))  # 6 місяців
MIN_DAYS_TO_BE_RATED = int(os.environ.get("MIN_DAYS_TO_BE_RATED", "180"))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
db = Database()


# ── FSM стани ──────────────────────────────────────────────────────────────

class RateStates(StatesGroup):
    waiting_score = State()
    waiting_review = State()


# ── Допоміжні функції ───────────────────────────────────────────────────────

def stars(score: float) -> str:
    full = int(round(score))
    return "★" * full + "☆" * (5 - full)


def format_member_since(joined: datetime) -> str:
    delta = datetime.utcnow() - joined
    months = delta.days // 30
    if months < 1:
        return f"{delta.days} дн."
    if months < 12:
        return f"{months} міс."
    years = months // 12
    m = months % 12
    return f"{years} р. {m} міс." if m else f"{years} р."


def score_kbd():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=str(i)) for i in range(1, 6)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def skip_kbd():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Пропустити")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


# ── Обробники подій групи ───────────────────────────────────────────────────

@dp.chat_member()
async def on_new_member(event):
    """Фіксуємо дату вступу кожного нового учасника."""
    new_status = event.new_chat_member.status
    user = event.new_chat_member.user
    if new_status in ("member", "administrator", "creator") and not user.is_bot:
        await db.register_member(
            user_id=user.id,
            username=user.username or "",
            full_name=user.full_name,
            chat_id=event.chat.id,
        )
        logger.info(f"Зареєстровано учасника: {user.id} ({user.username})")


# ── Команди ─────────────────────────────────────────────────────────────────

@dp.message(Command("start", "help"))
async def cmd_start(message: Message):
    text = (
        "🐔 <b>Рейтинг надійних постачальників</b>\n\n"
        "Доступні команди:\n"
        "/top — топ постачальників\n"
        "/check @username — профіль та відгуки\n"
        "/rate @username — залишити оцінку\n"
        "/mystatus — мій статус\n\n"
        f"<i>Для голосування потрібно бути в групі {MIN_DAYS_TO_VOTE // 30}+ місяців</i>"
    )
    await message.answer(text, parse_mode="HTML")


@dp.message(Command("top"))
async def cmd_top(message: Message):
    sellers = await db.get_top_sellers(limit=10)
    if not sellers:
        await message.answer("Поки немає оцінених постачальників.")
        return

    medals = ["🥇", "🥈", "🥉"] + ["4.", "5.", "6.", "7.", "8.", "9.", "10."]
    lines = ["<b>Топ надійних постачальників</b>\n"]
    for i, s in enumerate(sellers):
        medal = medals[i] if i < len(medals) else f"{i+1}."
        uname = f"@{s['username']}" if s['username'] else s['full_name']
        avg = s['avg_score']
        count = s['review_count']
        lines.append(
            f"{medal} {uname}  {stars(avg)} <b>{avg:.1f}</b>\n"
            f"   <i>{count} відгук(ів)</i>"
        )

    lines.append("\n<i>Оновлюється щопонеділка</i>")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("check"))
async def cmd_check(message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Вкажіть username: /check @vasyl_kury")
        return

    username = parts[1].lstrip("@")
    seller = await db.get_seller_profile(username)
    if not seller:
        await message.answer(f"@{username} не знайдено або ще не має оцінок.")
        return

    avg = seller['avg_score']
    count = seller['review_count']
    since = format_member_since(seller['joined_at'])
    uname = f"@{seller['username']}" if seller['username'] else seller['full_name']

    text = (
        f"<b>{uname}</b>  {stars(avg)} <b>{avg:.1f}/5</b>\n"
        f"В групі: {since} · Відгуків: {count}\n\n"
    )

    reviews = seller.get('recent_reviews', [])
    if reviews:
        text += "<b>Останні відгуки:</b>\n"
        for r in reviews:
            voter = f"@{r['voter_username']}" if r['voter_username'] else "учасник"
            sc = stars(r['score'])
            text += f"{sc} {voter}\n"
            if r['review_text']:
                text += f"<i>«{r['review_text']}»</i>\n"
            text += "\n"

    await message.answer(text.strip(), parse_mode="HTML")


@dp.message(Command("rate"))
async def cmd_rate(message: Message, state: FSMContext):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Вкажіть username: /rate @vasyl_kury")
        return

    target_username = parts[1].lstrip("@")

    # Перевірка: чи зареєстрований той, хто голосує
    voter = await db.get_member_by_id(message.from_user.id)
    if not voter:
        # Реєструємо на льоту якщо не знайдено
        await db.register_member(
            user_id=message.from_user.id,
            username=message.from_user.username or "",
            full_name=message.from_user.full_name,
            chat_id=message.chat.id,
        )
        voter = await db.get_member_by_id(message.from_user.id)

    days_in_group = (datetime.utcnow() - voter['joined_at']).days
    if days_in_group < MIN_DAYS_TO_VOTE:
        remaining = MIN_DAYS_TO_VOTE - days_in_group
        await message.answer(
            f"⏳ Ви ще не можете голосувати.\n"
            f"Потрібно ще <b>{remaining}</b> дн. в групі.",
            parse_mode="HTML"
        )
        return

    # Перевірка: чи існує продавець і чи достатньо давно в групі
    seller = await db.get_member_by_username(target_username)
    if not seller:
        await message.answer(f"@{target_username} не знайдено в групі.")
        return

    seller_days = (datetime.utcnow() - seller['joined_at']).days
    if seller_days < MIN_DAYS_TO_BE_RATED:
        await message.answer(
            f"@{target_username} ще не може отримувати оцінки — "
            f"в групі менше {MIN_DAYS_TO_BE_RATED // 30} місяців."
        )
        return

    if seller['user_id'] == message.from_user.id:
        await message.answer("Не можна оцінювати самого себе.")
        return

    # Перевірка: чи вже оцінював
    existing = await db.get_existing_rating(
        voter_id=message.from_user.id,
        seller_id=seller['user_id']
    )

    await state.update_data(
        seller_id=seller['user_id'],
        seller_username=target_username,
        is_update=existing is not None
    )
    await state.set_state(RateStates.waiting_score)

    prefix = "оновити оцінку для" if existing else "оцінити"
    uname = f"@{target_username}"
    await message.answer(
        f"Оцініть {uname} від 1 до 5:",
        reply_markup=score_kbd()
    )


@dp.message(RateStates.waiting_score)
async def process_score(message: Message, state: FSMContext):
    if message.text not in ["1", "2", "3", "4", "5"]:
        await message.answer("Оберіть оцінку від 1 до 5:", reply_markup=score_kbd())
        return

    await state.update_data(score=int(message.text))
    await state.set_state(RateStates.waiting_review)
    await message.answer(
        "Напишіть короткий відгук (або натисніть «Пропустити»):",
        reply_markup=skip_kbd()
    )


@dp.message(RateStates.waiting_review)
async def process_review(message: Message, state: FSMContext):
    data = await state.get_data()
    review_text = "" if message.text == "Пропустити" else message.text

    await db.save_rating(
        voter_id=message.from_user.id,
        voter_username=message.from_user.username or "",
        seller_id=data['seller_id'],
        score=data['score'],
        review_text=review_text,
        is_update=data['is_update']
    )

    await state.clear()
    action = "оновлено" if data['is_update'] else "збережено"
    uname = f"@{data['seller_username']}"
    await message.answer(
        f"✅ Вашу оцінку {action}!\n"
        f"Рейтинг {uname} оновлено.",
        reply_markup=ReplyKeyboardRemove()
    )


@dp.message(Command("mystatus"))
async def cmd_mystatus(message: Message):
    member = await db.get_member_by_id(message.from_user.id)
    if not member:
        await db.register_member(
            user_id=message.from_user.id,
            username=message.from_user.username or "",
            full_name=message.from_user.full_name,
            chat_id=message.chat.id,
        )
        member = await db.get_member_by_id(message.from_user.id)

    days = (datetime.utcnow() - member['joined_at']).days
    can_vote = days >= MIN_DAYS_TO_VOTE
    can_be_rated = days >= MIN_DAYS_TO_BE_RATED

    vote_status = "✅ Можете голосувати" if can_vote else f"⏳ До права голосу: {MIN_DAYS_TO_VOTE - days} дн."
    rate_status = "✅ Можете отримувати оцінки" if can_be_rated else f"⏳ До права на оцінки: {MIN_DAYS_TO_BE_RATED - days} дн."

    given = await db.count_ratings_given(message.from_user.id)
    received = await db.count_ratings_received(message.from_user.id)

    await message.answer(
        f"<b>Ваш статус</b>\n\n"
        f"В групі: {format_member_since(member['joined_at'])}\n"
        f"{vote_status}\n"
        f"{rate_status}\n\n"
        f"Оцінок надано: {given}\n"
        f"Оцінок отримано: {received}",
        parse_mode="HTML"
    )


# ── Адмін-команди ────────────────────────────────────────────────────────────

@dp.message(Command("admin_freeze"))
async def cmd_freeze(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Використання: /admin_freeze @username")
        return
    username = parts[1].lstrip("@")
    ok = await db.freeze_member(username)
    await message.answer(f"{'Заморожено' if ok else 'Не знайдено'}: @{username}")


@dp.message(Command("admin_unfreeze"))
async def cmd_unfreeze(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Використання: /admin_unfreeze @username")
        return
    username = parts[1].lstrip("@")
    ok = await db.unfreeze_member(username)
    await message.answer(f"{'Розморожено' if ok else 'Не знайдено'}: @{username}")


# ── Запуск ───────────────────────────────────────────────────────────────────

async def main():
    await db.init()
    logger.info("Бот запущено!")
    await dp.start_polling(bot, allowed_updates=["message", "chat_member"])


if __name__ == "__main__":
    asyncio.run(main())

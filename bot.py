import asyncio
import logging
import os
from datetime import datetime

from aiogram import Bot, Dispatcher
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

BOT_TOKEN            = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS            = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
MIN_DAYS_TO_VOTE     = int(os.environ.get("MIN_DAYS_TO_VOTE", "180"))
MIN_DAYS_TO_BE_RATED = int(os.environ.get("MIN_DAYS_TO_BE_RATED", "180"))

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())
db  = Database()


# ── FSM ────────────────────────────────────────────────────────────────────

class RateStates(StatesGroup):
    waiting_score  = State()
    waiting_review = State()


# ── Допоміжні функції ───────────────────────────────────────────────────────

def stars(score: float) -> str:
    full = int(round(score))
    return "★" * full + "☆" * (5 - full)


def format_since(joined: datetime) -> str:
    delta = datetime.utcnow() - joined
    months = delta.days // 30
    if months < 1:
        return f"{delta.days} дн."
    if months < 12:
        return f"{months} міс."
    years, m = divmod(months, 12)
    return f"{years} р. {m} міс." if m else f"{years} р."


def display_name(member: dict) -> str:
    if member.get("username"):
        return f"@{member['username']}"
    return member.get("full_name") or f"ID:{member['user_id']}"


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


async def ensure_registered(user, chat_id: int = 0) -> dict:
    member = await db.get_member_by_id(user.id)
    if not member:
        await db.register_member(
            user_id=user.id,
            username=user.username or "",
            full_name=user.full_name,
            chat_id=chat_id,
        )
        member = await db.get_member_by_id(user.id)
    return member


def has_voting_rights(member: dict) -> bool:
    launch = db.get_launch_date()
    if launch and member["joined_at"] <= launch:
        return True
    days = (datetime.utcnow() - member["joined_at"]).days
    return days >= MIN_DAYS_TO_VOTE


def can_receive_rating(member: dict) -> bool:
    launch = db.get_launch_date()
    if launch and member["joined_at"] <= launch:
        return True
    days = (datetime.utcnow() - member["joined_at"]).days
    return days >= MIN_DAYS_TO_BE_RATED


async def send_private(user_id: int, text: str, **kwargs) -> bool:
    """Надсилає повідомлення в приват. Повертає True якщо вдалось."""
    try:
        await bot.send_message(user_id, text, **kwargs)
        return True
    except Exception as e:
        logger.error(f"Не вдалось надіслати в приват {user_id}: {e}")
        return False


# ── Обробники подій групи ───────────────────────────────────────────────────

@dp.chat_member()
async def on_new_member(event):
    new_status = event.new_chat_member.status
    user = event.new_chat_member.user
    if new_status in ("member", "administrator", "creator") and not user.is_bot:
        await db.register_member(
            user_id=user.id,
            username=user.username or "",
            full_name=user.full_name,
            chat_id=event.chat.id,
        )
        logger.info(f"Зареєстровано: {user.id} ({user.username or user.full_name})")


# ── Команди ─────────────────────────────────────────────────────────────────

@dp.message(Command("start", "help"))
async def cmd_start(message: Message):
    await ensure_registered(message.from_user, message.chat.id)
    await message.answer(
        "🐔 <b>Рейтинг надійних постачальників</b>\n\n"
        "Доступні команди:\n"
        "/top — топ постачальників\n"
        "/check @username або ID — профіль та відгуки\n"
        "/rate — відповісти на повідомлення продавця щоб оцінити\n"
        "/members — список учасників з ID\n"
        "/mystatus — мій статус\n\n"
        f"<i>Для нових учасників: право голосу після {MIN_DAYS_TO_VOTE // 30} міс. в групі</i>",
        parse_mode="HTML",
    )


@dp.message(Command("top"))
async def cmd_top(message: Message):
    sellers = await db.get_top_sellers(limit=10)
    if not sellers:
        await message.answer("Поки немає оцінених постачальників.")
        return

    medals = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, 11)]
    lines = ["<b>Топ надійних постачальників</b>\n"]
    for i, s in enumerate(sellers):
        medal = medals[i] if i < len(medals) else f"{i+1}."
        uname = f"@{s['username']}" if s["username"] else s["full_name"]
        lines.append(
            f"{medal} {uname}  {stars(s['avg_score'])} <b>{s['avg_score']:.1f}</b>\n"
            f"   <i>{s['review_count']} відгук(ів)</i>"
        )
    lines.append("\n<i>Оновлюється щопонеділка</i>")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("check"))
async def cmd_check(message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Вкажіть username або ID:\n/check @vasyl_kury\n/check 123456789")
        return

    arg = parts[1].lstrip("@")
    seller = await db.get_member_by_id(int(arg)) if arg.isdigit() else await db.get_member_by_username(arg)

    if not seller:
        await message.answer("Учасника не знайдено.")
        return

    profile = await db.get_seller_profile(seller["user_id"])
    name = display_name(seller)

    if not profile or not profile.get("avg_score"):
        await message.answer(
            f"{name} ще не має оцінок.\nID: <code>{seller['user_id']}</code>",
            parse_mode="HTML"
        )
        return

    text = (
        f"<b>{name}</b>  {stars(profile['avg_score'])} <b>{profile['avg_score']:.1f}/5</b>\n"
        f"ID: <code>{seller['user_id']}</code>\n"
        f"В групі: {format_since(seller['joined_at'])} · Відгуків: {profile['review_count']}\n\n"
    )
    for r in profile.get("recent_reviews", []):
        voter = f"@{r['voter_username']}" if r["voter_username"] else "учасник"
        text += f"{stars(r['score'])} {voter}\n"
        if r["review_text"]:
            text += f"<i>«{r['review_text']}»</i>\n"
        text += "\n"

    await message.answer(text.strip(), parse_mode="HTML")


@dp.message(Command("members"))
async def cmd_members(message: Message):
    members = await db.get_all_members()
    if not members:
        await message.answer("Список порожній.")
        return
    lines = ["<b>Учасники групи</b>\n"]
    for m in members:
        lines.append(f"{display_name(m)} — <code>{m['user_id']}</code>")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("rate", ignore_mention=True))
async def cmd_rate(message: Message, state: FSMContext):
    # Якщо from_user None — анонімний адмін, ігноруємо
    if not message.from_user:
        await message.answer("❌ Анонімні адміни не можуть голосувати. Вимкніть анонімність в налаштуваннях групи.")
        return

    user_id  = message.from_user.id
    is_group = message.chat.type in ("group", "supergroup")

    voter = await ensure_registered(message.from_user, message.chat.id)

    if not has_voting_rights(voter):
        days      = (datetime.utcnow() - voter["joined_at"]).days
        remaining = MIN_DAYS_TO_VOTE - days
        text      = f"⏳ Ви ще не можете голосувати.\nПотрібно ще <b>{remaining}</b> дн. в групі."
        if is_group:
            sent = await message.answer(text, parse_mode="HTML")
            await asyncio.sleep(5)
            try:
                await sent.delete()
                await message.delete()
            except Exception:
                pass
        else:
            await message.answer(text, parse_mode="HTML")
        return

    # Визначаємо продавця
    seller = None
    if message.reply_to_message:
        replied_user = message.reply_to_message.from_user
        if replied_user and not replied_user.is_bot:
            seller = await ensure_registered(replied_user, message.chat.id)
    else:
        parts = message.text.split()
        if len(parts) >= 2:
            arg    = parts[1].lstrip("@")
            seller = await db.get_member_by_id(int(arg)) if arg.isdigit() else await db.get_member_by_username(arg)

    if not seller:
        help_text = (
            "Як оцінити продавця:\n\n"
            "1️⃣ Знайдіть повідомлення продавця в групі\n"
            "2️⃣ Натисніть «Відповісти» на його повідомлення\n"
            "3️⃣ Напишіть /rate\n\n"
            "Або: /rate @username · /rate 123456789"
        )
        await message.answer(help_text)
        return

    if seller["user_id"] == user_id:
        await message.answer("Не можна оцінювати самого себе. 😄")
        return

    if not can_receive_rating(seller):
        days      = (datetime.utcnow() - seller["joined_at"]).days
        remaining = MIN_DAYS_TO_BE_RATED - days
        await message.answer(
            f"{display_name(seller)} ще не може отримувати оцінки.\n"
            f"Потрібно ще {remaining} дн. в групі."
        )
        return

    existing = await db.get_existing_rating(voter_id=user_id, seller_id=seller["user_id"])

    await state.update_data(
        seller_id=seller["user_id"],
        seller_name=display_name(seller),
        is_update=existing is not None,
        from_group=is_group,
    )
    await state.set_state(RateStates.waiting_score)

    action   = "Оновіть оцінку" if existing else "Оцініть"
    msg_text = f"{action} <b>{display_name(seller)}</b> від 1 до 5:"

    if is_group:
        # Пробуємо написати в приват
        ok = await send_private(user_id, msg_text, reply_markup=score_kbd(), parse_mode="HTML")
        if ok:
            # Видаляємо команду з групи і пишемо підказку на 5 секунд
            try:
                await message.delete()
            except Exception:
                pass
            hint = await message.answer("📩 Перевірте особисті повідомлення від бота!")
            await asyncio.sleep(5)
            try:
                await hint.delete()
            except Exception:
                pass
        else:
            await message.answer(
                "⚠️ Спочатку напишіть боту /start в приват:\n"
                f"@{(await bot.get_me()).username}\n\n"
                "Потім повторіть /rate"
            )
            await state.clear()
    else:
        # Вже в приваті — відповідаємо тут
        await message.answer(msg_text, reply_markup=score_kbd(), parse_mode="HTML")


@dp.message(RateStates.waiting_score)
async def process_score(message: Message, state: FSMContext):
    if message.text not in ["1", "2", "3", "4", "5"]:
        await message.answer("Оберіть оцінку від 1 до 5:", reply_markup=score_kbd())
        return
    await state.update_data(score=int(message.text))
    await state.set_state(RateStates.waiting_review)
    await message.answer(
        "Напишіть короткий відгук (або натисніть «Пропустити»):",
        reply_markup=skip_kbd(),
    )


@dp.message(RateStates.waiting_review)
async def process_review(message: Message, state: FSMContext):
    data        = await state.get_data()
    review_text = "" if message.text == "Пропустити" else message.text

    await db.save_rating(
        voter_id=message.from_user.id,
        voter_username=message.from_user.username or "",
        seller_id=data["seller_id"],
        score=data["score"],
        review_text=review_text,
        is_update=data["is_update"],
    )
    await state.clear()

    action = "оновлено" if data["is_update"] else "збережено"
    await message.answer(
        f"✅ Вашу оцінку {action}!\n"
        f"Рейтинг {data['seller_name']} оновлено.",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(Command("mystatus"))
async def cmd_mystatus(message: Message):
    if not message.from_user:
        return
    member = await ensure_registered(message.from_user, message.chat.id)
    days   = (datetime.utcnow() - member["joined_at"]).days

    vote_status = "✅ Можете голосувати" if has_voting_rights(member) \
        else f"⏳ До права голосу: {MIN_DAYS_TO_VOTE - days} дн."
    rate_status = "✅ Можете отримувати оцінки" if can_receive_rating(member) \
        else f"⏳ До права на оцінки: {MIN_DAYS_TO_BE_RATED - days} дн."

    given    = await db.count_ratings_given(message.from_user.id)
    received = await db.count_ratings_received(message.from_user.id)

    await message.answer(
        f"<b>Ваш статус</b>\n\n"
        f"В групі: {format_since(member['joined_at'])}\n"
        f"ID: <code>{member['user_id']}</code>\n"
        f"{vote_status}\n"
        f"{rate_status}\n\n"
        f"Оцінок надано: {given}\n"
        f"Оцінок отримано: {received}",
        parse_mode="HTML",
    )


# ── Адмін-команди ────────────────────────────────────────────────────────────

@dp.message(Command("admin_freeze"))
async def cmd_freeze(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS:
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Використання: /admin_freeze @username або ID")
        return
    arg = parts[1].lstrip("@")
    ok  = await db.freeze_member_by_id(int(arg)) if arg.isdigit() else await db.freeze_member(arg)
    await message.answer("Заморожено ✅" if ok else "Не знайдено ❌")


@dp.message(Command("admin_unfreeze"))
async def cmd_unfreeze(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS:
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Використання: /admin_unfreeze @username або ID")
        return
    arg = parts[1].lstrip("@")
    ok  = await db.unfreeze_member_by_id(int(arg)) if arg.isdigit() else await db.unfreeze_member(arg)
    await message.answer("Розморожено ✅" if ok else "Не знайдено ❌")


# ── Запуск ───────────────────────────────────────────────────────────────────

async def main():
    await db.init()
    bot_info = await bot.get_me()
    logger.info(f"Бот запущено! @{bot_info.username}")
    await dp.start_polling(bot, allowed_updates=["message", "chat_member"])


if __name__ == "__main__":
    asyncio.run(main())

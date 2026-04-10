import aiosqlite
import os
from datetime import datetime
from typing import Optional

DB_PATH = os.environ.get("DB_PATH", "rating.db")


class Database:
    def __init__(self):
        self.db_path = DB_PATH

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS members (
                    user_id     INTEGER PRIMARY KEY,
                    username    TEXT,
                    full_name   TEXT,
                    chat_id     INTEGER,
                    joined_at   TEXT DEFAULT (datetime('now')),
                    is_frozen   INTEGER DEFAULT 0
                )
            """)
            
            # Міграція: додаємо full_name, якщо таблиця вже існувала без цієї колонки
            try:
                await db.execute("ALTER TABLE members ADD COLUMN full_name TEXT")
                await db.commit()
            except aiosqlite.OperationalError:
                # Колонка вже існує
                pass

            await db.execute("""
                CREATE TABLE IF NOT EXISTS ratings (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    voter_id        INTEGER NOT NULL,
                    voter_username  TEXT,
                    seller_id       INTEGER NOT NULL,
                    score           INTEGER NOT NULL CHECK(score BETWEEN 1 AND 5),
                    review_text     TEXT DEFAULT '',
                    created_at      TEXT DEFAULT (datetime('now')),
                    updated_at      TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY(voter_id)  REFERENCES members(user_id),
                    FOREIGN KEY(seller_id) REFERENCES members(user_id),
                    UNIQUE(voter_id, seller_id)
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_ratings_seller ON ratings(seller_id)")
            await db.commit()

    # ── Учасники ──────────────────────────────────────────────────────────────

    async def register_member(self, user_id: int, username: str, full_name: str, chat_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO members (user_id, username, full_name, chat_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username  = excluded.username,
                    full_name = excluded.full_name
            """, (user_id, username, full_name, chat_id))
            await db.commit()

    async def get_member_by_id(self, user_id: int) -> Optional[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM members WHERE user_id = ? AND is_frozen = 0", (user_id,)
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return None
                d = dict(row)
                d['joined_at'] = datetime.fromisoformat(d['joined_at'])
                return d

    async def get_member_by_username(self, username: str) -> Optional[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM members WHERE username = ? AND is_frozen = 0", (username,)
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return None
                d = dict(row)
                d['joined_at'] = datetime.fromisoformat(d['joined_at'])
                return d

    async def freeze_member(self, username: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "UPDATE members SET is_frozen = 1 WHERE username = ?", (username,)
            )
            await db.commit()
            return cur.rowcount > 0

    async def unfreeze_member(self, username: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "UPDATE members SET is_frozen = 0 WHERE username = ?", (username,)
            )
            await db.commit()
            return cur.rowcount > 0

    # ── Рейтинги ─────────────────────────────────────────────────────────────

    async def get_existing_rating(self, voter_id: int, seller_id: int) -> Optional[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM ratings WHERE voter_id = ? AND seller_id = ?",
                (voter_id, seller_id)
            ) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    async def save_rating(self, voter_id: int, voter_username: str,
                          seller_id: int, score: int,
                          review_text: str, is_update: bool):
        async with aiosqlite.connect(self.db_path) as db:
            if is_update:
                await db.execute("""
                    UPDATE ratings
                    SET score = ?, review_text = ?, voter_username = ?,
                        updated_at = datetime('now')
                    WHERE voter_id = ? AND seller_id = ?
                """, (score, review_text, voter_username, voter_id, seller_id))
            else:
                await db.execute("""
                    INSERT INTO ratings (voter_id, voter_username, seller_id, score, review_text)
                    VALUES (?, ?, ?, ?, ?)
                """, (voter_id, voter_username, seller_id, score, review_text))
            await db.commit()

    async def get_top_sellers(self, limit: int = 10) -> list:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT m.user_id, m.username, m.full_name,
                       ROUND(AVG(r.score), 1) AS avg_score,
                       COUNT(r.id)            AS review_count
                FROM ratings r
                JOIN members m ON m.user_id = r.seller_id
                WHERE m.is_frozen = 0
                GROUP BY r.seller_id
                HAVING review_count >= 1
                ORDER BY avg_score DESC, review_count DESC
                LIMIT ?
            """, (limit,)) as cur:
                return [dict(row) for row in await cur.fetchall()]

    async def get_seller_profile_by_id(self, user_id: int) -> Optional[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT m.user_id, m.username, m.full_name, m.joined_at,
                       ROUND(AVG(r.score), 1) AS avg_score,
                       COUNT(r.id)            AS review_count
                FROM members m
                LEFT JOIN ratings r ON r.seller_id = m.user_id
                WHERE m.user_id = ? AND m.is_frozen = 0
                GROUP BY m.user_id
            """, (user_id,)) as cur:
                row = await cur.fetchone()
                if not row or row['avg_score'] is None:
                    return None
                result = dict(row)
                result['joined_at'] = datetime.fromisoformat(result['joined_at'])

            # Останні 5 відгуків
            async with db.execute("""
                SELECT r.score, r.review_text, r.voter_username, r.updated_at
                FROM ratings r
                JOIN members m ON m.user_id = r.seller_id
                WHERE m.user_id = ?
                ORDER BY r.updated_at DESC
                LIMIT 5
            """, (user_id,)) as cur:
                result['recent_reviews'] = [dict(row) for row in await cur.fetchall()]

            return result

    async def count_ratings_given(self, user_id: int) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM ratings WHERE voter_id = ?", (user_id,)
            ) as cur:
                return (await cur.fetchone())[0]

    async def count_ratings_received(self, user_id: int) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM ratings WHERE seller_id = ?", (user_id,)
            ) as cur:
                return (await cur.fetchone())[0]

import pandas as pd
import psycopg2
from datetime import datetime
import re

# ----config----
CSV_PATH = "games.csv"

conn = psycopg2.connect(
    dbname="DesafioBancoDeDados",
    user="postgres",
    password="teste123",
    host="localhost"
)

cur = conn.cursor()
df = pd.read_csv(CSV_PATH)

# ---cache---
cache = {
    "developers": {},
    "publishers": {},
    "genres": {},
    "categories": {},
    "tags": {},
    "languages": {},
    "platforms": {}
}

# --- funções ---
def clean(value):
    if pd.isna(value):
        return None
    return value

def parse_owners(value):
    if pd.isna(value):
        return (None, None)
    
    value = str(value).replace('"', '').strip()

    try:
        parts = value.split('-')
        min_val = int(parts[0].strip())
        max_val = int(parts[1].strip())
        return (min_val, max_val)
    except:
        return (None, None)

def split_values(value):
    if pd.isna(value):
        return []
    
    value = str(value).replace('"', '').strip()
    return [v.strip() for v in re.split(r'[;,]', value) if v.strip()]

def parse_date(date_str):
    if pd.isna(date_str):
        return None
    for fmt in ("%b %d, %Y", "%b %Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except:
            continue
    return None


def get_or_create(table, value):
    if not value or str(value).strip() == "":
        return None

    value = value.strip()

    # cache
    if value in cache[table]:
        return cache[table][value]

    # banco
    cur.execute(f"SELECT id FROM {table} WHERE name = %s", (value,))
    result = cur.fetchone()

    if result:
        cache[table][value] = result[0]
        return result[0]

    # insert
    cur.execute(f"INSERT INTO {table} (name) VALUES (%s) RETURNING id", (value,))
    new_id = cur.fetchone()[0]

    cache[table][value] = new_id
    return new_id


# ----loop principal----
for i, row in df.iterrows():

    game_id = int(row['AppID'])

    # PROGRESSO
    if i % 500 == 0:
        print(f"{i} jogos processados")

    # games
    owners_min, owners_max = parse_owners(row['Estimated owners'])
    cur.execute("""
        INSERT INTO games (
            appid, name, release_date, estimated_owners_min, estimated_owners_max,
            peak_ccu, required_age, price, discount, dlc_count,
            metacritic_score, user_score, positive, negative,
            score_rank, achievements, recommendations,
            average_playtime_forever, average_playtime_2weeks,
            median_playtime_forever, median_playtime_2weeks
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (appid) DO NOTHING;
    """, (
        game_id,
        row['Name'],
        parse_date(row['Release date']),
        owners_min,
        owners_max,
        row['Peak CCU'],
        row['Required age'],
        clean(row['Price']),
        row['Discount'],
        row['DLC count'],
        clean(row['Metacritic score']),
        clean(row['User score']),
        row['Positive'],
        row['Negative'],
        row['Score rank'],
        row['Achievements'],
        row['Recommendations'],
        row['Average playtime forever'],
        row['Average playtime two weeks'],
        row['Median playtime forever'],
        row['Median playtime two weeks']
    ))

    # game details
    cur.execute("""
        INSERT INTO game_details (
            appid, about, notes, reviews, website,
            support_url, support_email, header_image, metacritic_url
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (appid) DO NOTHING;
    """, (
        game_id,
        row['About the game'],
        row['Notes'],
        row['Reviews'],
        clean(row['Website']),
        clean(row['Support url']),
        clean(row['Support email']),
        clean(row['Header image']),
        clean(row['Metacritic url'])
    ))

    # platafomrs
    platforms = {
        'Windows': row['Windows'],
        'Mac': row['Mac'],
        'Linux': row['Linux']
    }

    for name, value in platforms.items():
        if value:
            pid = get_or_create("platforms", name)
            cur.execute("""
                INSERT INTO game_platforms (game_id, platform_id)
                VALUES (%s,%s)
                ON CONFLICT DO NOTHING;
            """, (game_id, pid))

    # developers
    if pd.notna(row['Developers']):
        for dev in split_values(row['Developers']):
            did = get_or_create("developers", dev)
            if did:
                cur.execute("""
                    INSERT INTO game_developers (game_id, developer_id)
                    VALUES (%s,%s)
                    ON CONFLICT DO NOTHING;
                """, (game_id, did))

    # publishers
    if pd.notna(row['Publishers']):
        for pub in split_values(row['Publishers']):
            pid = get_or_create("publishers", pub)
            if pid:
                cur.execute("""
                    INSERT INTO game_publishers (game_id, publisher_id)
                    VALUES (%s,%s)
                    ON CONFLICT DO NOTHING;
                """, (game_id, pid))

    # genres
    if pd.notna(row['Genres']):
        for genre in split_values(row['Genres']):
            gid = get_or_create("genres", genre)
            if gid:
                cur.execute("""
                    INSERT INTO game_genres (game_id, genre_id)
                    VALUES (%s,%s)
                    ON CONFLICT DO NOTHING;
                """, (game_id, gid))

    # categories
    if pd.notna(row['Categories']):
        for cat in split_values(row['Categories']):
            cid = get_or_create("categories", cat)
            if cid:
                cur.execute("""
                    INSERT INTO game_categories (game_id, category_id)
                    VALUES (%s,%s)
                    ON CONFLICT DO NOTHING;
                """, (game_id, cid))

    # tags
    if pd.notna(row['Tags']):
        for tag in split_values(row['Tags']):
            tid = get_or_create("tags", tag)
            if tid:
                cur.execute("""
                    INSERT INTO game_tags (game_id, tag_id)
                    VALUES (%s,%s)
                    ON CONFLICT DO NOTHING;
                """, (game_id, tid))

    # languages (SUPPORTED)
    if pd.notna(row['Supported languages']):
        for lang in split_values(row['Supported languages']):
            lid = get_or_create("languages", lang)
            if lid:
                cur.execute("""
                    INSERT INTO game_languages (game_id, language_id, type)
                    VALUES (%s,%s,'supported')
                    ON CONFLICT DO NOTHING;
                """, (game_id, lid))

    # languages (AUDIO)
    if pd.notna(row['Full audio languages']):
        for lang in split_values(row['Full audio languages']):
            lid = get_or_create("languages", lang)
            if lid:
                cur.execute("""
                    INSERT INTO game_languages (game_id, language_id, type)
                    VALUES (%s,%s,'audio')
                    ON CONFLICT DO NOTHING;
                """, (game_id, lid))

    # screenshots
    if pd.notna(row['Screenshots']):
        for sc in split_values(row['Screenshots']):
            cur.execute("""
                INSERT INTO screenshots (game_id, url)
                VALUES (%s,%s)
            """, (game_id, sc.strip()))

    # movies
    if pd.notna(row['Movies']):
        for mv in split_values(row['Movies']):
            cur.execute("""
                INSERT INTO movies (game_id, url)
                VALUES (%s,%s)
            """, (game_id, mv.strip()))

    # commit em bloco
    if i % 1000 == 0:
        conn.commit()

# ===== FINAL =====
conn.commit()
cur.close()
conn.close()

print("BANCO POPULADOOOOO 100% COM SUCESSO!")

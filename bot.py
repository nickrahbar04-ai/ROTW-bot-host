import os
import re
import json
import random
import sqlite3
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import aiohttp
import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv

# ----------------------------
# Setup
# ----------------------------

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID_AJET = os.getenv("AIRTABLE_BASE_ID_AJET")
AIRTABLE_BASE_ID_CODESHARE = os.getenv("AIRTABLE_BASE_ID_CODESHARE")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))
ROTW_CHANNEL_ID = int(os.getenv("ROTW_CHANNEL_ID", "0"))
ROTW_ROLE_ID = int(os.getenv("ROTW_ROLE_ID", "0"))
ROTW_STAFF_ROLE_ID = int(os.getenv("ROTW_STAFF_ROLE_ID", "0"))

if not DISCORD_TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN")
if not AIRTABLE_TOKEN:
    raise RuntimeError("Missing AIRTABLE_TOKEN")
if not AIRTABLE_BASE_ID_AJET:
    raise RuntimeError("Missing AIRTABLE_BASE_ID_AJET")
if not AIRTABLE_BASE_ID_CODESHARE:
    raise RuntimeError("Missing AIRTABLE_BASE_ID_CODESHARE")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rotw_bot")

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# Change these to your real table names
AJET_TABLE = "AJet Route Table"


DB_PATH = "rotw.db"

DEFAULT_CONFIG = {
    "ajet_count": 12,
    "codeshare_count": 15,
    "recent_weeks_block": 6,
}


def get_base_url(table_name: str) -> str:
    if table_name == AJET_TABLE:
        return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID_AJET}/{table_name}"
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID_CODESHARE}/{table_name}"


# ----------------------------
# Database
# ----------------------------

def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS rotw_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        week_start TEXT NOT NULL,
        route_key TEXT NOT NULL,
        route_number TEXT,
        source TEXT NOT NULL,
        partner TEXT NOT NULL,
        departure_code TEXT NOT NULL,
        arrival_code TEXT NOT NULL,
        aircraft TEXT,
        UNIQUE(week_start, route_key)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS bot_config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """)

    for key, value in DEFAULT_CONFIG.items():
        cur.execute(
            "INSERT OR IGNORE INTO bot_config (key, value) VALUES (?, ?)",
            (key, str(value)),
        )

    conn.commit()
    conn.close()


def get_config() -> dict:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM bot_config")
    rows = cur.fetchall()
    conn.close()

    config = {}
    for key, value in rows:
        try:
            config[key] = int(value)
        except ValueError:
            config[key] = value
    return config


def set_config_value(key: str, value: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO bot_config (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )
    conn.commit()
    conn.close()


def save_rotw_history(week_start: str, routes: list[dict]) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for route in routes:
        cur.execute("""
        INSERT OR IGNORE INTO rotw_history (
            week_start, route_key, route_number, source, partner,
            departure_code, arrival_code, aircraft
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            week_start,
            route["route_key"],
            route["route_number"],
            route["source"],
            route["partner"],
            route["departure_code"],
            route["arrival_code"],
            route["aircraft"],
        ))

    conn.commit()
    conn.close()


def get_recent_route_keys(weeks: int) -> set[str]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cutoff = (datetime.now(timezone.utc) - timedelta(weeks=weeks)).date().isoformat()
    cur.execute(
        "SELECT DISTINCT route_key FROM rotw_history WHERE week_start >= ?",
        (cutoff,),
    )
    rows = cur.fetchall()
    conn.close()
    return {row[0] for row in rows}


def get_last_history(limit: int = 20) -> list[tuple]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT week_start, route_number, partner, departure_code, arrival_code, aircraft
        FROM rotw_history
        ORDER BY week_start DESC, partner ASC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


# ----------------------------
# Helpers
# ----------------------------

def get_rotw_preview_summary(routes: list[dict]) -> str:
    ajet_count = 0
    codeshare_count = 0
    partners = set()

    for route in routes:
        source = route.get("source", "").lower()

        if source == "ajet":
            ajet_count += 1
        elif source == "codeshare":
            codeshare_count += 1

            partner = route.get("partner")
            if partner:
                partners.add(partner)

    return (
        f"✈️ **AJet routes:** {ajet_count}\n"
        f"🤝 **Codeshare routes:** {codeshare_count}\n"
        f"🌐 **Codeshare partners:** {len(partners)}"
    )


def has_rotw_staff_access(interaction: discord.Interaction) -> bool:
    if ROTW_STAFF_ROLE_ID == 0:
        return False

    member = interaction.user

    if not isinstance(member, discord.Member):
        return False

    return any(role.id == ROTW_STAFF_ROLE_ID for role in member.roles)


async def require_rotw_staff(interaction: discord.Interaction) -> bool:
    if has_rotw_staff_access(interaction):
        return True

    if interaction.response.is_done():
        await interaction.followup.send(
            "You do not have permission to use this ROTW management command.",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            "You do not have permission to use this ROTW management command.",
            ephemeral=True,
        )

    return False


def normalize_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()

def extract_best_code(value: str) -> str:
    if not value:
        return ""

    text = value.upper().strip()
    parts = re.split(r"[\/,\-\s]+", text)
    parts = [p for p in parts if p]

    iata = next((p for p in parts if len(p) == 3 and p.isalpha()), None)
    icao = next((p for p in parts if len(p) == 4 and p.isalpha()), None)

    return iata or icao or text


def extract_icao(value: str) -> str | None:
    if not value:
        return None

    text = value.upper().strip()
    parts = re.split(r"[\/,\-\s]+", text)
    for part in parts:
        if len(part) == 4 and part.isalpha():
            return part
    return None


def build_route_key(dep: str, arr: str) -> str:
    return f"{dep.upper()}-{arr.upper()}"


def is_valid_route(route: dict) -> bool:
    return bool(
        route["route_number"]
        and route["departure_code"]
        and route["arrival_code"]
        and route["route_key"]
    )

def format_duration(seconds):
    try:
        seconds = int(seconds)
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}:{minutes:02d}"
    except (TypeError, ValueError):
        return seconds

def target_week_start() -> str:
    today = datetime.now(timezone.utc).date()

    # Monday = 0, Sunday = 6
    # On Sunday, ROTW is for the upcoming Monday-Sunday week.
    if today.weekday() == 6:
        monday = today + timedelta(days=1)
    else:
        monday = today - timedelta(days=today.weekday())

    return monday.isoformat()


def week_range_text(week_start: str) -> str:
    monday = datetime.strptime(week_start, "%Y-%m-%d").date()
    sunday = monday + timedelta(days=6)

    return (
        f"**Monday {monday.strftime('%d.%m.%y')} → "
        f"Sunday {sunday.strftime('%d.%m.%y')}**\n"
        f"**00:00 → 23:59**"
    )

    # Monday = 0, Sunday = 6
    # If today is Sunday, show the NEXT Monday-Sunday week
    if today.weekday() == 6:
        monday = today + timedelta(days=1)
    else:
        monday = today - timedelta(days=today.weekday())

    sunday = monday + timedelta(days=6)

    return (
        f"**Monday {monday.strftime('%d.%m.%y')} → Sunday {sunday.strftime('%d.%m.%y')}**\n"
        f"**00:00 → 23:59**"
    )

def split_embed_field_lines(lines: list[str], max_length: int = 1024) -> list[str]:
    chunks = []
    current = ""

    for line in lines:
        # +1 for newline
        if len(current) + len(line) + 1 > max_length:
            if current:
                chunks.append(current)
            current = line
        else:
            if current:
                current += "\n" + line
            else:
                current = line

    if current:
        chunks.append(current)

    return chunks
    


# ----------------------------
# Airtable client
# ----------------------------

async def fetch_all_records(
    session: aiohttp.ClientSession,
    table_name: str,
    fields: list[str]
) -> list[dict]:

    url = get_base_url(table_name)
    logger.info("FETCHING TABLE: %s", table_name)
    logger.info("FETCHING URL: %s", url)

    records = []
    offset = None

    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}"
    }

    while True:
        params = []
        for field in fields:
            params.append(("fields[]", field))

        if offset:
            params.append(("offset", offset))

        url = get_base_url(table_name)

        async with session.get(url, headers=headers, params=params) as response:
            if response.status != 200:
                text = await response.text()
                raise RuntimeError(
                    f"Airtable error {response.status} for table '{table_name}': {text}"
                )

            data = await response.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")

            if not offset:
                break

    return records


async def fetch_all_routes() -> tuple[list[dict], list[dict]]:
    ajet_routes = []
    codeshare_routes = []

    logger.info("=== FETCHING ROUTES STARTED ===")

    async with aiohttp.ClientSession() as session:
        ajet_fields = [
            "Route Number",
            "Origin (IATA/ICAO)",
            "Destination (IATA/ICAO)",
            "Flight Time",
            "Aircraft Used",
            "Remarks",
        ]

        ajet_records = await fetch_all_records(session, AJET_TABLE, ajet_fields)
        for record in ajet_records:
            route = normalize_ajet(record)
            if is_valid_route(route):
                ajet_routes.append(route)

        logger.info("=== FETCHING CODESHARES ===")

        discovered_codeshare_tables = await fetch_codeshare_tables(session)


        for table in discovered_codeshare_tables:
            partner_name = table["partner"]
            table_name = table["name"]
            field_names = table["field_names"]

            logger.info("PROCESSING TABLE: %s", table_name)

            codeshare_fields = [
                "Flight Number",
                "Departure ICAO",
                "Arrival ICAO",
                "Arrival Airport",
                "Aircraft",
            ]

            if "Departure Airport" in field_names:
                codeshare_fields.append("Departure Airport")
            elif "Daperture Airport" in field_names:
                codeshare_fields.append("Daperture Airport")

            if "Flighttime" in field_names:
                codeshare_fields.append("Flighttime")
            elif "Flightttime" in field_names:
                codeshare_fields.append("Flightttime")

            records = await fetch_all_records(session, table_name, codeshare_fields)

            for record in records:
                route = normalize_codeshare(record, partner_name)
                if is_valid_route(route):
                    codeshare_routes.append(route)

        logger.info("AJET ROUTES: %s", len(ajet_routes))
        logger.info("CODESHARE ROUTES: %s", len(codeshare_routes))

    return ajet_routes, codeshare_routes

async def fetch_codeshare_tables(session: aiohttp.ClientSession) -> list[dict]:
    url = f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID_CODESHARE}/tables"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}"
    }

    async with session.get(url, headers=headers) as response:
        if response.status != 200:
            text = await response.text()
            raise RuntimeError(
                f"Failed to fetch codeshare table metadata: {response.status} {text}"
            )

        data = await response.json()

    valid_tables = []

    for table in data.get("tables", []):
        table_name = table.get("name", "")
        fields = table.get("fields", [])
        field_names = {field.get("name", "") for field in fields}

        logger.info("TABLE: %s", table_name)
        logger.info("FIELDS: %s", field_names)

        required_base_fields = {
            "Flight Number",
            "Departure ICAO",
            "Arrival ICAO",
            "Arrival Airport",
            "Aircraft",
        }

        missing_fields = required_base_fields - field_names

        has_departure_airport = (
            "Departure Airport" in field_names
            or "Daperture Airport" in field_names
        )

        has_time_field = (
            "Flighttime" in field_names
            or "Flightttime" in field_names
        )

        if missing_fields:
            logger.warning(
                "SKIPPING TABLE '%s' - missing required fields: %s",
                table_name,
                ", ".join(sorted(missing_fields)),
            )
            continue

        if not has_departure_airport:
            logger.warning(
                "SKIPPING TABLE '%s' - missing Departure Airport field",
                table_name,
            )
            continue

        if not has_time_field:
            logger.warning(
                "SKIPPING TABLE '%s' - missing Flighttime field",
                table_name,
            )
            continue

        valid_tables.append({
            "name": table_name,
            "partner": table_name.replace(" Routes", "").strip(),
            "field_names": field_names,
        })

        logger.info(
            "VALID CODESHARE TABLE: %s",
            table_name,
        )

        logger.info(
        "CODESHARE DISCOVERY COMPLETE: %d valid tables",
        len(valid_tables),
        )

        logger.info(
        "VALID CODESHARE TABLES: %s",
        [table["name"] for table in valid_tables],
        )

    return valid_tables



class ROTWPreviewView(discord.ui.View):
    def __init__(self, routes: list[dict], week_start: str):
        super().__init__(timeout=600)
        self.routes = routes
        self.week_start = week_start

    @discord.ui.button(
        label="Post ROTW",
        style=discord.ButtonStyle.green,
        emoji="✅",
    )
    async def post_rotw_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if not await require_rotw_staff(interaction):
            return

        await interaction.response.defer(ephemeral=True)

        try:
            channel = bot.get_channel(ROTW_CHANNEL_ID)

            if channel is None:
                await interaction.followup.send(
                    "Unable to find the ROTW channel.",
                    ephemeral=True,
                )
                return

            await publish_rotw(
                channel,
                self.routes,
                self.week_start,
            )

            await interaction.followup.send(
                "ROTW posted successfully.",
                ephemeral=True,
            )

        except Exception as e:
            logger.exception("Error posting ROTW from preview")

            await interaction.followup.send(
                f"Unable to post ROTW: `{e}`",
                ephemeral=True,
            )

    @discord.ui.button(
        label="Regenerate Preview",
        style=discord.ButtonStyle.secondary,
        emoji="🔄",
    )
    async def regenerate_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if not await require_rotw_staff(interaction):
            return

        await interaction.response.defer()

        try:
            routes, week_start = await generate_rotw()

            self.routes = routes
            self.week_start = week_start

            embed = format_rotw_embed(routes, week_start)

            embed.insert_field_at(
                0,
                name="📊 Preview Summary",
                value=get_rotw_preview_summary(routes),
                inline=False,
            )

            await interaction.edit_original_response(
                content="Generated a new ROTW preview.",
                embed=embed,
                view=self,
            )

        except Exception as e:
            logger.exception("Error regenerating ROTW preview")

            await interaction.followup.send(
                f"Unable to regenerate preview: `{e}`",
                ephemeral=True,
            )

# ----------------------------
# Normalizers
# ----------------------------

def normalize_ajet(record: dict) -> dict:
    fields = record.get("fields", {})

    origin_raw = normalize_text(fields.get("Origin (IATA/ICAO)"))
    destination_raw = normalize_text(fields.get("Destination (IATA/ICAO)"))

    departure_code = extract_best_code(origin_raw)
    arrival_code = extract_best_code(destination_raw)

    return {
        "source": "ajet",
        "partner": "AJet Virtual",
        "route_number": normalize_text(fields.get("Route Number")),
        "departure_code": departure_code,
        "arrival_code": arrival_code,
        "departure_icao": extract_icao(origin_raw),
        "arrival_icao": extract_icao(destination_raw),
        "departure_airport": None,
        "arrival_airport": None,
        "aircraft": normalize_text(fields.get("Aircraft Used")),
        "flight_time": normalize_text(fields.get("Flight Time")),
        "remarks": normalize_text(fields.get("Remarks")),
        "route_key": build_route_key(departure_code, arrival_code),
    }


def normalize_codeshare(record: dict, partner_name: str) -> dict:
    fields = record.get("fields", {})

    departure_icao = normalize_text(fields.get("Departure ICAO")).upper()
    arrival_icao = normalize_text(fields.get("Arrival ICAO")).upper()

    return {
        "source": "codeshare",
        "partner": partner_name,
        "route_number": normalize_text(fields.get("Flight Number")),
        "departure_code": departure_icao,
        "arrival_code": arrival_icao,
        "departure_icao": departure_icao,
        "arrival_icao": arrival_icao,
        "departure_airport": normalize_text(fields.get("Departure Airport") or fields.get("Daperture Airport")),
        "arrival_airport": normalize_text(fields.get("Arrival Airport")),
        "aircraft": normalize_text(fields.get("Aircraft")),
        "flight_time": normalize_text(fields.get("Flighttime") or fields.get("Flightttime")),
        "remarks": None,
        "route_key": build_route_key(departure_icao, arrival_icao),
    }
# ----------------------------
# Selection logic
# ----------------------------

def deduplicate_routes(routes: list[dict]) -> list[dict]:
    seen = set()
    unique_routes = []

    for route in routes:
        if route["route_key"] in seen:
            continue
        seen.add(route["route_key"])
        unique_routes.append(route)

    return unique_routes


def pick_rotw_routes(
    ajet_routes: list[dict],
    codeshare_routes: list[dict],
    ajet_count: int,
    codeshare_count: int,
    recent_keys: set[str],
) -> list[dict]:
    random.shuffle(ajet_routes)
    random.shuffle(codeshare_routes)

    selected = []
    used_keys = set()
    used_partners = set()

    for route in ajet_routes:
        if len([r for r in selected if r["source"] == "ajet"]) >= ajet_count:
            break
        if route["route_key"] in recent_keys or route["route_key"] in used_keys:
            continue

        selected.append(route)
        used_keys.add(route["route_key"])

    for route in codeshare_routes:
        if len([r for r in selected if r["source"] == "codeshare"]) >= codeshare_count:
            break
        if route["route_key"] in recent_keys or route["route_key"] in used_keys:
            continue
        if route["partner"] in used_partners:
            continue

        selected.append(route)
        used_keys.add(route["route_key"])
        used_partners.add(route["partner"])

    for route in codeshare_routes:
        if len([r for r in selected if r["source"] == "codeshare"]) >= codeshare_count:
            break
        if route["route_key"] in recent_keys or route["route_key"] in used_keys:
            continue

        selected.append(route)
        used_keys.add(route["route_key"])

    return selected


def format_rotw_embed(routes: list[dict], week_start: str) -> discord.Embed:
    embed = discord.Embed(
        title="✈️ Route of the Week",
        description=week_range_text(week_start),
        color=discord.Color.blue(),
    )

    ajet_lines = []
    codeshare_lines = []

    ajet_routes = [r for r in routes if r["source"] == "ajet"]
    codeshare_routes = [r for r in routes if r["source"] == "codeshare"]

    # Keep AJet in selected order
    for route in ajet_routes:
        line = (
            f"**{route['route_number']}** — "
            f"`{route['departure_code']} → {route['arrival_code']}` — "
            f"{route['aircraft'] or 'Unknown aircraft'}"
        )

        if route["flight_time"]:
            line += f" — {format_duration(route['flight_time'])}"

        ajet_lines.append(line)

    # Group codeshares by airline
    grouped_codeshares = defaultdict(list)

    for route in codeshare_routes:
        grouped_codeshares[route["partner"]].append(route)

    # Build grouped output
    for partner in sorted(grouped_codeshares.keys()):
        codeshare_lines.append(f"__**{partner}**__")

        for route in sorted(grouped_codeshares[partner], key=lambda r: r["route_number"]):
            line = (
                f"**{route['route_number']}** — "
                f"`{route['departure_code']} → {route['arrival_code']}` — "
                f"{route['aircraft'] or 'Unknown aircraft'}"
            )

            if route["flight_time"]:
                line += f" — {format_duration(route['flight_time'])}"

            codeshare_lines.append(line)

        codeshare_lines.append("")

    # AJet fields
    ajet_chunks = split_embed_field_lines(ajet_lines)

    if ajet_chunks:
        for i, chunk in enumerate(ajet_chunks):
            embed.add_field(
                name="AJet Virtual" if i == 0 else "AJet Virtual Continued",
                value=chunk,
                inline=False
            )
    else:
        embed.add_field(
            name="AJet Virtual",
            value="No routes selected.",
            inline=False
    )

# Codeshare fields
    codeshare_chunks = split_embed_field_lines(codeshare_lines)

    if codeshare_chunks:
        for i, chunk in enumerate(codeshare_chunks):
            embed.add_field(
                name="Codeshare Partners" if i == 0 else "Codeshare Partners Continued",
                value=chunk,
                inline=False
            )
    else:
        embed.add_field(
            name="Codeshare Partners",
            value="No routes selected.",
            inline=False
        )

    embed.set_footer(text="Generated automatically from Airtable")
    return embed

async def generate_rotw() -> tuple[list[dict], str]:
    config = get_config()
    recent_keys = get_recent_route_keys(config["recent_weeks_block"])

    ajet_routes, codeshare_routes = await fetch_all_routes()

    ajet_routes = deduplicate_routes(ajet_routes)
    codeshare_routes = deduplicate_routes(codeshare_routes)

    routes = pick_rotw_routes(
        ajet_routes=ajet_routes,
        codeshare_routes=codeshare_routes,
        ajet_count=config["ajet_count"],
        codeshare_count=config["codeshare_count"],
        recent_keys=recent_keys,
    )

    week_start = target_week_start()
    return routes, week_start

async def publish_rotw(
    channel: discord.abc.Messageable,
    routes: list[dict],
    week_start: str,
) -> discord.Message:
    embed = format_rotw_embed(routes, week_start)

    content = f"<@&{ROTW_ROLE_ID}>" if ROTW_ROLE_ID else None

    msg = await channel.send(
        content=content,
        embed=embed,
        allowed_mentions=discord.AllowedMentions(
            roles=True,
            everyone=False,
            users=False,
        ),
    )

    for reaction in ("🔥", "✈️", "❤️"):
        try:
            await msg.add_reaction(reaction)
        except discord.HTTPException:
            logger.warning("Failed to add reaction %s", reaction)

    save_rotw_history(week_start, routes)

    return msg


# ----------------------------
# Discord commands
# ----------------------------

@bot.event
async def on_ready():
    logger.info("Logged in as %s (%s)", bot.user, bot.user.id)

    try:
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
        else:
            synced = await bot.tree.sync()

        logger.info("Synced %d app commands", len(synced))
    except Exception:
        logger.exception("Failed to sync app commands")

    if not weekly_rotw_task.is_running():
        weekly_rotw_task.start()


@bot.tree.command(name="rotw_generate", description="Generate a new ROTW preview")
async def rotw_generate(interaction: discord.Interaction):
    if not await require_rotw_staff(interaction):
        return

    await interaction.response.defer(thinking=True, ephemeral=True)

    try:
        routes, week_start = await generate_rotw()
        embed = format_rotw_embed(routes, week_start)

        embed.insert_field_at(
            0,
            name="📊 Preview Summary",
            value=get_rotw_preview_summary(routes),
            inline=False,
        )

        preview_data = {
            "week_start": week_start,
            "routes": routes,
        }
        with open("rotw_preview.json", "w", encoding="utf-8") as f:
            json.dump(preview_data, f, ensure_ascii=False, indent=2)

        await interaction.followup.send(
            "Generated a new ROTW preview.",
            embed=embed,
            view=ROTWPreviewView(routes, week_start),
            ephemeral=True,
        )

    except Exception as e:
        logger.exception("Error generating ROTW")
        await interaction.followup.send(f"Error generating ROTW: `{e}`", ephemeral=True)


@bot.tree.command(name="rotw_post", description="Post the current ROTW preview to the configured channel")
async def rotw_post(interaction: discord.Interaction):
    if not await require_rotw_staff(interaction):
        return
    await interaction.response.defer(thinking=True, ephemeral=True)

    try:
        if not os.path.exists("rotw_preview.json"):
            await interaction.followup.send(
                "No preview found. Use `/rotw_generate` first.",
                ephemeral=True,
            )
            return

        with open("rotw_preview.json", "r", encoding="utf-8") as f:
            preview_data = json.load(f)

        week_start = preview_data["week_start"]
        routes = preview_data["routes"]

        channel = bot.get_channel(ROTW_CHANNEL_ID)
        if channel is None:
            await interaction.followup.send(
                "ROTW channel not found. Check `ROTW_CHANNEL_ID`.",
                ephemeral=True,
            )
            return

        await publish_rotw(channel, routes, week_start)

        await interaction.followup.send("Generated a new ROTW preview.",
            embed=embed,
            view=ROTWPreviewView(),
            ephemeral=True
        )

    except Exception as e:
        logger.exception("Error posting ROTW")
        await interaction.followup.send(f"Error posting ROTW: `{e}`", ephemeral=True)


@bot.tree.command(name="rotw_history", description="Show recent ROTW history")
async def rotw_history(interaction: discord.Interaction):
    rows = get_last_history(15)

    if not rows:
        await interaction.response.send_message("No ROTW history yet.", ephemeral=True)
        return

    lines = []
    for week_start, route_number, partner, dep, arr, aircraft in rows:
        lines.append(
            f"**{week_start}** — {route_number} — `{dep} → {arr}` — {aircraft} — *{partner}*"
        )

    await interaction.response.send_message("\n".join(lines[:15]), ephemeral=True)

@bot.tree.command(
    name="rotw_status",
    description="Show ROTW bot status, route counts and configuration"
)
async def rotw_status(interaction: discord.Interaction):
    if not await require_rotw_staff(interaction):
        return 

    await interaction.response.defer(thinking=True, ephemeral=True)

    try:
        config = get_config()
        week_start = target_week_start()

        ajet_routes, codeshare_routes = await fetch_all_routes()

        ajet_routes = deduplicate_routes(ajet_routes)
        codeshare_routes = deduplicate_routes(codeshare_routes)

        partner_counts = defaultdict(int)

        for route in codeshare_routes:
            partner_counts[route["partner"]] += 1

        embed = discord.Embed(
            title="🛠️ AJet ROTW Bot Status",
            description=week_range_text(week_start),
            color=discord.Color.green(),
        )

        embed.add_field(
            name="✈️ Route Database",
            value=(
                f"**AJet routes:** {len(ajet_routes)}\n"
                f"**Codeshare routes:** {len(codeshare_routes)}\n"
                f"**Codeshare partners:** {len(partner_counts)}"
            ),
            inline=False,
        )

        embed.add_field(
            name="⚙️ Current Configuration",
            value=(
                f"**AJet selections:** {config['ajet_count']}\n"
                f"**Codeshare selections:** {config['codeshare_count']}\n"
                f"**Repeat protection:** {config['recent_weeks_block']} weeks"
            ),
            inline=False,
        )

        partner_lines = [
            f"**{partner}** — {count} routes"
            for partner, count in sorted(partner_counts.items())
        ]

        partner_chunks = split_embed_field_lines(
            partner_lines,
            max_length=1000,
        )

        for i, chunk in enumerate(partner_chunks):
            embed.add_field(
                name=(
                    "🤝 Codeshare Partners"
                    if i == 0
                    else "🤝 Codeshare Partners Continued"
                ),
                value=chunk,
                inline=False,
            )

        embed.set_footer(
            text="Live data retrieved from Airtable"
        )

        await interaction.followup.send(
            embed=embed,
            ephemeral=True,
        )

    except Exception as e:
        logger.exception("Error retrieving ROTW status")

        await interaction.followup.send(
            f"Unable to retrieve ROTW status: `{e}`",
            ephemeral=True,
        )


@bot.tree.command(name="rotw_settings", description="Change route counts and duplicate block window")
@app_commands.describe(
    ajet_count="How many AJet routes to select",
    codeshare_count="How many codeshare routes to select",
    recent_weeks_block="How many weeks to block repeats for",
)
async def rotw_settings(
    interaction: discord.Interaction,
    ajet_count: int,
    codeshare_count: int,
    recent_weeks_block: int,
):
    if not await require_rotw_staff(interaction):
        return
    
    if ajet_count < 0 or codeshare_count < 0 or recent_weeks_block < 0:
        await interaction.response.send_message("Values must be 0 or higher.", ephemeral=True)
        return

    set_config_value("ajet_count", ajet_count)
    set_config_value("codeshare_count", codeshare_count)
    set_config_value("recent_weeks_block", recent_weeks_block)

    await interaction.response.send_message(
        f"Settings updated:\n"
        f"- AJet routes: **{ajet_count}**\n"
        f"- Codeshare routes: **{codeshare_count}**\n"
        f"- Repeat block: **{recent_weeks_block} weeks**",
        ephemeral=True,
    )


# ----------------------------
# Weekly scheduler
# ----------------------------

@tasks.loop(minutes=30)
async def weekly_rotw_task():
    now = datetime.now(timezone.utc)

    if now.weekday() != 6:
        return

    if now.hour != 9:
        return

    week_start = target_week_start()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM rotw_history WHERE week_start = ?", (week_start,))
    count = cur.fetchone()[0]
    conn.close()

    if count > 0:
        return

    try:
        routes, week_start = await generate_rotw()
        if not routes:
            logger.warning("No routes generated for ROTW")
            return

        channel = bot.get_channel(ROTW_CHANNEL_ID)
        if channel is None:
            logger.warning("ROTW channel not found")
            return

        await publish_rotw(channel, routes, week_start)

        logger.info("Automatically posted ROTW for %s", week_start)

    except Exception:
        logger.exception("Failed automatic ROTW post")


@weekly_rotw_task.before_loop
async def before_weekly_task():
    await bot.wait_until_ready()


# ----------------------------
# Main
# ----------------------------

def main():
    init_db()
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
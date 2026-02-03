# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb"]
# ///
"""
Setup script for NYC Restaurant Cuisine Dashboard.
Downloads subway station data, loads CSVs into DuckDB, classifies cuisines,
computes nearest stations, and creates aggregated tables.
"""

import urllib.request
import os
import duckdb

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
SUBWAY_CSV = os.path.join(DATA_DIR, "subway_stations.csv")
RESTAURANT_CSV = os.path.join(DATA_DIR, "Open_Restaurants_Inspections_20260107.csv")
CUISINE_LOOKUP_TSV = os.path.join(DATA_DIR, "cuisine_lookup.tsv")
DB_PATH = os.path.join(DATA_DIR, "nyc_food.duckdb")
SUBWAY_URL = "https://data.ny.gov/api/views/39hk-dx4f/rows.csv?accessType=DOWNLOAD"

# Each tuple is (cuisine_label, regex_pattern). Order matters: first match wins.
CUISINES = [
    # ── Specific food type (before Italian so pizza shops get their own category) ──
    ("Pizza", r"pizza|pizzeria"),

    # ── East Asian ──
    ("Chinese",
     r"chin(a|ese)|wok|dumpling|dim.?sum|szechuan|sichuan|hunan|peking|shanghai|"
     r"cantonese|chow|kung|lo.?mein|\bbao\b|congee|hotpot|hot.?pot|wonton|won.?ton|"
     r"\bpanda\b|mandarin|\bjade\b|chopstick|great.?wall|golden.?dragon"),
    ("Japanese",
     r"sushi|ramen|japanese|\bjapan\b|teriyaki|tempura|\budon\b|\bsoba\b|izakaya|"
     r"yakitori|tonkatsu|omakase|hibachi|sakura"),
    ("Korean", r"korean|\bkorea\b|bibimbap|kimchi|bulgogi|galbi"),
    ("Taiwanese", r"taiwan(ese)?|bubble.?tea|\bboba\b"),
    ("Mongolian", r"mongol(ia|ian)"),

    # ── Southeast Asian ──
    ("Thai", r"\bthai\b|thailand|bangkok|pad.?thai|\bsiam\b"),
    ("Vietnamese", r"vietnamese|vietnam|\bpho\b|banh.?mi|saigon|hanoi"),
    ("Filipino", r"filipin(o|a)|philippin(e|es)|lumpia|manila|pinoy"),
    ("Malaysian", r"malaysia(n)?|nasi.?lemak"),
    ("Indonesian", r"indonesia(n)?|nasi.?goreng|rendang|jakarta"),
    ("Cambodian", r"cambodia(n)?|khmer|phnom.?penh"),
    ("Burmese", r"burm(a|ese)|myanmar"),
    ("Laotian", r"lao(s|tian)"),
    ("Singaporean", r"singapore(an)?"),
    ("Bruneian", r"brunei(an)?"),
    ("Timorese", r"timor(ese)?"),

    # ── South Asian ──
    ("Indian",
     r"indian|\bindia\b|\bcurry\b|tandoori|masala|biryani|\bnaan\b|\bdosa\b|tikka|"
     r"paneer|samosa|chutney|bombay|\bdelhi\b|punjab|himalaya|vindaloo|korma"),
    ("Pakistani", r"pakistan(i)?|karachi|lahori"),
    ("Bangladeshi", r"bangladesh(i)?|bengali|\bdhaka\b"),
    ("Sri Lankan", r"sri.?lank(a|an)|ceylon(ese)?"),
    ("Nepali", r"nepal(i|ese)?|kathmandu|\bmomo\b"),
    ("Bhutanese", r"bhutan(ese)?"),
    ("Maldivian", r"maldiv(es|ian)"),

    # ── Central Asian ──
    ("Afghan", r"afghan(i|istan)?|\bkabul\b"),
    ("Uzbek", r"uzbek(istan)?|\bplov\b|samarkand"),
    ("Kazakh", r"kazakh(stan)?"),
    ("Kyrgyz", r"kyrgyz(stan)?"),
    ("Tajik", r"tajik(istan)?"),
    ("Turkmen", r"turkmen(istan)?"),

    # ── Middle Eastern ──
    ("Turkish", r"turk(ish|ey|iye)|döner|doner|\bkebab\b|baklava|istanbul|ankara"),
    ("Lebanese", r"leban(on|ese)|beirut"),
    ("Persian", r"persia(n)?|iran(ian)?|tehran"),
    ("Israeli", r"israel(i)?|tel.?aviv|jerusalem"),
    ("Iraqi", r"iraq(i)?|baghdad"),
    ("Syrian", r"syria(n)?|damascus|aleppo"),
    ("Jordanian", r"jordan(ian)?|\bamman\b"),
    ("Yemeni", r"yemen(i)?"),
    ("Saudi", r"saudi|arabia(n)?"),
    ("Emirati", r"emirat(i|es)|dubai|abu.?dhabi"),
    ("Kuwaiti", r"kuwait(i)?"),
    ("Omani", r"\boman(i)?\b"),
    ("Bahraini", r"bahrain(i)?"),
    ("Qatari", r"qatar(i)?"),
    ("Palestinian", r"palestin(e|ian)"),

    # ── North African ──
    ("Moroccan", r"morocc(o|an)|\bmaroc\b|tagine|marrakech"),
    ("Egyptian", r"egypt(ian)?|cairo|koshary"),
    ("Tunisian", r"tunis(ia|ian)?"),
    ("Algerian", r"alger(ia|ian)?"),
    ("Libyan", r"liby(a|an)"),
    ("Sudanese", r"sudan(ese)?|khartoum"),

    # ── West African ──
    ("Nigerian", r"nigeria(n)?|jollof|lagos|\bsuya\b"),
    ("Ghanaian", r"ghan(a|ian)|accra"),
    ("Senegalese", r"senegal(ese)?|dakar"),
    ("Malian", r"\bmali(an)?\b|bamako"),
    ("Ivorian", r"ivory.?coast|ivoire|ivorian|abidjan"),
    ("Guinean", r"\bguinea(n)?\b|conakry"),
    ("Sierra Leonean", r"sierra.?leon(e|ean)"),
    ("Liberian", r"liberi(a|an)"),
    ("Togolese", r"togo(lese)?|lom[eé]"),
    ("Beninese", r"benin(ese)?|cotonou"),
    ("Burkinabe", r"burkina|burkinab[eé]"),
    ("Gambian", r"\bgambi(a|an)\b"),
    ("Cape Verdean", r"cape.?verd(e|ean)|cabo.?verde"),
    ("Mauritanian", r"mauritani(a|an)"),
    ("Nigerien", r"\bniger(ien)?\b"),

    # ── East African ──
    ("Ethiopian", r"ethiopi(a|an)|injera|berbere|addis"),
    ("Eritrean", r"eritre(a|an)"),
    ("Somali", r"somali(a|an)?|mogadishu"),
    ("Kenyan", r"keny(a|an)|nairobi"),
    ("Tanzanian", r"tanzani(a|an)|zanzibar|dar.?es.?salaam"),
    ("Ugandan", r"ugand(a|an)|kampala"),
    ("Rwandan", r"rwand(a|an)|kigali"),
    ("Burundian", r"burund(i|ian)"),
    ("Djiboutian", r"djibouti(an)?"),
    ("South Sudanese", r"south.?sudan(ese)?"),

    # ── Central African ──
    ("Congolese", r"congol(ese)?|\bcongo\b|kinshasa"),
    ("Cameroonian", r"cameroon(ian)?|douala|yaound[eé]"),
    ("Chadian", r"\bchad(ian)?\b"),
    ("Gabonese", r"gabon(ese)?|libreville"),
    ("Central African", r"central.?african|bangui"),
    ("Equatorial Guinean", r"equatorial.?guinea"),
    ("Sao Tomean", r"s[aã]o.?tom[eé]"),

    # ── Southern African ──
    ("South African", r"south.?african|braai|cape.?town|johannesburg"),
    ("Zimbabwean", r"zimbabwe(an)?|harare"),
    ("Mozambican", r"mozambi(can|que)|maputo"),
    ("Zambian", r"zambi(a|an)|lusaka"),
    ("Malawian", r"malawi(an)?|lilongwe"),
    ("Botswanan", r"botswana(n)?|gaborone"),
    ("Namibian", r"namibi(a|an)|windhoek"),
    ("Angolan", r"angol(a|an)|luanda"),
    ("Malagasy", r"madagascar|malagasy"),
    ("Mauritian", r"mauriti(us|an)"),
    ("Swazi", r"eswatini|swazi(land)?"),
    ("Lesothan", r"lesotho|basotho"),
    ("Seychellois", r"seychell(es|ois)"),
    ("Comorian", r"comor(os|ian)"),

    # ── Caribbean (specific countries before generic) ──
    ("Jamaican", r"jamaica(n)?|\bjerk\b|kingston"),
    ("Cuban", r"cuba(n|no)?|\bcuba\b|havana|\bhabana\b"),
    ("Haitian", r"haiti(an)?"),
    ("Dominican", r"dominican|santo.?domingo"),
    ("Puerto Rican", r"puerto.?ric(o|an)|boricua"),
    ("Trinidadian", r"trinidad(ian)?|tobago"),
    ("Barbadian", r"barbad(os|ian)|bajan"),
    ("Bahamian", r"bahama(s|ian)"),
    ("Grenadian", r"grenad(a|ian)"),
    ("St. Lucian", r"st\.?.?luci(a|an)"),
    ("Antiguan", r"antigu(a|an)"),
    ("St. Vincentian", r"st\.?.?vincent"),
    ("Kittitian", r"st\.?.?kitts|nevis"),
    ("Dominican (Dominica)", r"\bdominica\b"),
    ("Caribbean", r"caribbean|\broti\b|plantain|oxtail"),

    # ── Mexican ──
    ("Mexican",
     r"mexican|\bmexico\b|méxico|\btaco\b|burrito|taqueria|enchilada|tortilla|"
     r"tamale|quesadilla|cantina|oaxaca|puebla|jalisco|azteca"),

    # ── Central American ──
    ("Guatemalan", r"guatemal(a|an|teco)"),
    ("Belizean", r"beliz(e|ean)"),
    ("Honduran", r"hondur(as|an)"),
    ("Salvadoran", r"salvad(or|oran)|pupusa|el.?salvador"),
    ("Nicaraguan", r"nicaragu(a|an)"),
    ("Costa Rican", r"costa.?ric(a|an)"),
    ("Panamanian", r"panam(a|á|anian)"),

    # ── South American ──
    ("Peruvian", r"peru(vian)?|\bperú\b|ceviche|lomo.?saltado|anticucho"),
    ("Colombian", r"colombi(a|an)|arepa|bandeja|bogot[aá]|medell[ií]n"),
    ("Brazilian", r"brazil(ian)?|brasil(eiro)?|churrasco|açaí|\bacai\b|feijoada"),
    ("Argentine", r"argentin(a|e|ian)|buenos.?aires|asado|gaucho|chimichurri"),
    ("Venezuelan", r"venezuel(a|an)|caracas"),
    ("Chilean", r"chile(an)?|santiago"),
    ("Ecuadorian", r"ecuador(ian)?|quito"),
    ("Bolivian", r"bolivi(a|an)"),
    ("Paraguayan", r"paragua(y|yan)"),
    ("Uruguayan", r"urugua(y|yan)|montevideo"),
    ("Guyanese", r"guyan(a|ese)"),
    ("Surinamese", r"surinam(e|ese)"),

    # ── Western European ──
    ("Italian",
     r"italian|\bitalia\b|trattoria|ristorante|osteria|\bpasta\b|lasagna|gelato|"
     r"panini|focaccia|gnocchi|il.?forno|napoli|romano"),
    ("French",
     r"french|\bfrance\b|bistro|brasserie|creperie|crêperie|patisserie|pâtisserie"),
    ("Spanish", r"spanish|\bspain\b|españa|tapas|paella|bodega|sangria"),
    ("Portuguese", r"portugu(ese|al)|lisbon|lisboa|\bnando\b"),
    ("German",
     r"german(y)?|deutsch|biergarten|schnitzel|bratwurst|sauerkraut|pretzel|"
     r"bavaria|münchen"),
    ("Austrian", r"austri(a|an)|vienna|\bwien\b"),
    ("Swiss", r"swiss|switzerland|fondue|raclette|zürich"),
    ("Belgian", r"belgi(an|um)|brussels|bruxelles|waffle"),
    ("Dutch", r"dutch|holland|netherlands|amsterdam"),
    ("Luxembourgish", r"luxemb(ourg|ourgish)"),

    # ── British Isles ──
    ("British", r"british|english|\bengland\b"),
    ("Irish", r"irish|\bireland\b|dublin"),
    ("Scottish", r"scotl(and|ish)|edinburgh"),
    ("Welsh", r"\bwales\b|welsh"),

    # ── Nordic ──
    ("Swedish", r"swed(ish|en)|stockholm"),
    ("Norwegian", r"norweg(ian|y)|\boslo\b"),
    ("Danish", r"danish|denmark|copenhagen|smørrebrød"),
    ("Finnish", r"finn(ish|land)|helsinki"),
    ("Icelandic", r"iceland(ic)?|reykjavik"),

    # ── Eastern European & Balkans ──
    ("Greek/Mediterranean",
     r"greek|\bgreece\b|hellas|gyro|souvlaki|mediterranean|falafel|shawarma|"
     r"hummus|\bpita\b"),
    ("Polish", r"polish|poland|polska|pierogi|kielbasa"),
    ("Russian", r"russi(a|an)|moscow|borscht|blini|pelmeni"),
    ("Ukrainian", r"ukrain(e|ian)|kyiv|kiev|varenyky"),
    ("Hungarian", r"hungar(y|ian)|budapest|goulash"),
    ("Czech", r"czech|prague|praha|bohemian"),
    ("Romanian", r"romani(a|an)|bucharest"),
    ("Bulgarian", r"bulgari(a|an)|\bsofia\b"),
    ("Serbian", r"serbi(a|an)|belgrade"),
    ("Croatian", r"croat(ia|ian)|zagreb"),
    ("Bosnian", r"bosni(a|an)|sarajevo"),
    ("Slovenian", r"sloven(ia|ian)|ljubljana"),
    ("Slovak", r"slovak(ia|ian)?|bratislava"),
    ("Lithuanian", r"lithuan(ia|ian)|vilnius"),
    ("Latvian", r"latvi(a|an)|\briga\b"),
    ("Estonian", r"estoni(a|an)|tallinn"),
    ("Belarusian", r"belarus(ian)?|minsk"),
    ("Moldovan", r"moldov(a|an)"),
    ("Albanian", r"albani(a|an)|tirana"),
    ("Macedonian", r"macedoni(a|an)|skopje"),
    ("Montenegrin", r"montenegr(o|in)"),
    ("Kosovar", r"kosov(o|ar)"),

    # ── Caucasus ──
    ("Georgian", r"georgi(a|an)|tbilisi|khachapuri|khinkali"),
    ("Armenian", r"armeni(a|an)|yerevan"),
    ("Azerbaijani", r"azerbai(jan|jani)|\bbaku\b"),

    # ── Oceania & Pacific ──
    ("Australian", r"australi(a|an)"),
    ("New Zealand", r"new.?zealand"),
    ("Hawaiian", r"hawai(i|ian)|\bpoke\b|\baloha\b"),
    ("Fijian", r"fiji(an)?"),
    ("Samoan", r"samoa(n)?"),
    ("Tongan", r"tonga(n)?"),
    ("Papua New Guinean", r"papua|new.?guinea"),
    ("Pacific Islander", r"polynesia(n)?|melanesia(n)?|micronesia(n)?"),
]


def _build_cuisine_case_sql():
    """Generate the SQL CASE statement from the CUISINES list."""
    clauses = []
    for label, pattern in CUISINES:
        escaped = label.replace("'", "''")
        clauses.append(
            f"WHEN regexp_matches(LOWER(d.RestaurantName), '{pattern}') "
            f"THEN '{escaped}'"
        )
    body = "\n                ".join(clauses)
    return f"CASE\n                {body}\n                ELSE NULL\n            END"


def download_subway_data():
    if os.path.exists(SUBWAY_CSV):
        print(f"Subway CSV already exists at {SUBWAY_CSV}, skipping download.")
        return
    print("Downloading subway station data...")
    urllib.request.urlretrieve(SUBWAY_URL, SUBWAY_CSV)
    print(f"Downloaded to {SUBWAY_CSV}")


def build_database():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    con = duckdb.connect(DB_PATH)

    # ---- Load raw CSVs ----
    print("Loading restaurant CSV...")
    con.execute(f"""
        CREATE TABLE raw_restaurants AS
        SELECT * FROM read_csv_auto('{RESTAURANT_CSV}')
    """)
    row_count = con.execute("SELECT COUNT(*) FROM raw_restaurants").fetchone()[0]
    print(f"  Loaded {row_count:,} raw restaurant rows")

    print("Loading subway CSV...")
    con.execute(f"""
        CREATE TABLE raw_subway AS
        SELECT * FROM read_csv_auto('{SUBWAY_CSV}')
    """)
    row_count = con.execute("SELECT COUNT(*) FROM raw_subway").fetchone()[0]
    print(f"  Loaded {row_count:,} raw subway rows")

    print("Loading cuisine lookup table...")
    con.execute(f"""
        CREATE TABLE cuisine_lookup AS
        SELECT
            column0 AS restaurant_name,
            column1 AS cuisine
        FROM read_csv('{CUISINE_LOOKUP_TSV}',
            delim='\t', header=false, columns={{'column0': 'VARCHAR', 'column1': 'VARCHAR'}})
    """)
    lookup_count = con.execute("SELECT COUNT(*) FROM cuisine_lookup").fetchone()[0]
    print(f"  Loaded {lookup_count:,} cuisine lookup entries")

    # ---- Deduplicate subway stations by Complex ID ----
    print("Deduplicating subway stations by Complex ID...")
    con.execute("""
        CREATE TABLE subway_stations AS
        SELECT
            "Complex ID" AS complex_id,
            FIRST("Stop Name") AS stop_name,
            AVG(CAST("GTFS Latitude" AS DOUBLE)) AS latitude,
            AVG(CAST("GTFS Longitude" AS DOUBLE)) AS longitude,
            STRING_AGG(DISTINCT "Daytime Routes", ' ' ORDER BY "Daytime Routes") AS all_routes
        FROM raw_subway
        GROUP BY "Complex ID"
    """)
    station_count = con.execute("SELECT COUNT(*) FROM subway_stations").fetchone()[0]
    print(f"  {station_count:,} unique station complexes")

    # ---- Deduplicate restaurants and classify cuisines ----
    cuisine_case = _build_cuisine_case_sql()
    print("Deduplicating restaurants and classifying cuisines...")
    con.execute(f"""
        CREATE TABLE classified_restaurants AS
        WITH deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY RestaurantName, BusinessAddress
                    ORDER BY InspectedOn DESC
                ) AS rn
            FROM raw_restaurants
            WHERE Latitude IS NOT NULL
              AND Longitude IS NOT NULL
              AND CAST(Latitude AS DOUBLE) != 0
              AND CAST(Longitude AS DOUBLE) != 0
        )
        SELECT
            d.RestaurantName AS restaurant_name,
            d.BusinessAddress AS address,
            d.Borough AS borough,
            d.Postcode AS postcode,
            CAST(d.Latitude AS DOUBLE) AS latitude,
            CAST(d.Longitude AS DOUBLE) AS longitude,
            COALESCE(
                cl.cuisine,
                {cuisine_case},
                'Unclassified'
            ) AS cuisine
        FROM deduped d
        LEFT JOIN cuisine_lookup cl
            ON LOWER(TRIM(d.RestaurantName)) = LOWER(TRIM(cl.restaurant_name))
        WHERE d.rn = 1
    """)

    total = con.execute("SELECT COUNT(*) FROM classified_restaurants").fetchone()[0]
    classified = con.execute(
        "SELECT COUNT(*) FROM classified_restaurants WHERE cuisine != 'Unclassified'"
    ).fetchone()[0]
    print(f"  {total:,} deduplicated restaurants, {classified:,} classified ({classified*100//total}%)")

    # ---- Nearest station via LATERAL join ----
    print("Computing nearest subway station for each restaurant...")
    con.execute("""
        CREATE TABLE restaurants_with_station AS
        SELECT
            r.restaurant_name,
            r.address,
            r.borough,
            r.postcode,
            r.latitude,
            r.longitude,
            r.cuisine,
            s.stop_name AS station_name,
            s.latitude AS station_lat,
            s.longitude AS station_lon,
            s.all_routes AS station_routes
        FROM classified_restaurants r,
        LATERAL (
            SELECT stop_name, latitude, longitude, all_routes, complex_id
            FROM subway_stations
            ORDER BY (r.latitude - latitude)*(r.latitude - latitude)
                   + (r.longitude - longitude)*(r.longitude - longitude)
            LIMIT 1
        ) s
    """)

    rws_count = con.execute("SELECT COUNT(*) FROM restaurants_with_station").fetchone()[0]
    print(f"  {rws_count:,} restaurants with nearest station assigned")

    # ---- Station cuisine summary ----
    print("Building station cuisine counts...")
    con.execute("""
        CREATE TABLE station_cuisine_counts AS
        SELECT
            station_name,
            station_lat,
            station_lon,
            station_routes,
            cuisine,
            COUNT(*) AS restaurant_count
        FROM restaurants_with_station
        GROUP BY station_name, station_lat, station_lon, station_routes, cuisine
    """)

    scc_count = con.execute("SELECT COUNT(*) FROM station_cuisine_counts").fetchone()[0]
    print(f"  {scc_count:,} station-cuisine combinations")

    # ---- Print stats ----
    print("\n--- Cuisine Distribution ---")
    rows = con.execute("""
        SELECT cuisine, COUNT(*) AS cnt
        FROM restaurants_with_station
        GROUP BY cuisine
        ORDER BY cnt DESC
    """).fetchall()
    for cuisine, cnt in rows:
        print(f"  {cuisine:<25} {cnt:>5}")

    print(f"\n  Total: {sum(r[1] for r in rows):,}")

    con.close()
    print(f"\nDatabase written to {DB_PATH}")


if __name__ == "__main__":
    download_subway_data()
    build_database()

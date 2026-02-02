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
DB_PATH = os.path.join(DATA_DIR, "nyc_food.duckdb")
SUBWAY_URL = "https://data.ny.gov/api/views/39hk-dx4f/rows.csv?accessType=DOWNLOAD"


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
    print("Deduplicating restaurants and classifying cuisines...")
    con.execute("""
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
            RestaurantName AS restaurant_name,
            BusinessAddress AS address,
            Borough AS borough,
            Postcode AS postcode,
            CAST(Latitude AS DOUBLE) AS latitude,
            CAST(Longitude AS DOUBLE) AS longitude,
            CASE
                WHEN regexp_matches(LOWER(RestaurantName),
                    'chin(a|ese)|wok|dumpling|dim.?sum|szechuan|sichuan|hunan|peking|shanghai|cantonese|chow|kung|lo.?mein|\bbao\b|congee|hotpot|hot.?pot|wonton|won.?ton|\bpanda\b|mandarin|\bjade\b|chopstick|great.?wall|golden.?dragon')
                    THEN 'Chinese'
                WHEN regexp_matches(LOWER(RestaurantName),
                    'pizza|pizzeria|trattoria|ristorante|italian|osteria|\bpasta\b|lasagna|gelato|panini|focaccia|gnocchi|il.?forno|napoli|romano')
                    THEN 'Italian'
                WHEN regexp_matches(LOWER(RestaurantName),
                    '\btaco|burrito|mexican|taqueria|enchilada|tortilla|tamale|quesadilla|cantina|oaxaca|puebla|jalisco|azteca')
                    THEN 'Mexican'
                WHEN regexp_matches(LOWER(RestaurantName),
                    'sushi|ramen|japanese|teriyaki|tempura|\budon\b|\bsoba\b|izakaya|yakitori|tonkatsu|omakase|hibachi|sakura')
                    THEN 'Japanese'
                WHEN regexp_matches(LOWER(RestaurantName),
                    'indian|\bcurry\b|tandoori|masala|biryani|\bnaan\b|\bdosa\b|tikka|paneer|samosa|chutney|bombay|\bdelhi\b|punjab|himalaya|vindaloo|korma')
                    THEN 'Indian'
                WHEN regexp_matches(LOWER(RestaurantName),
                    '\bthai\b|bangkok|pad.?thai|\bsiam\b')
                    THEN 'Thai'
                WHEN regexp_matches(LOWER(RestaurantName),
                    'korean|bibimbap|kimchi|bulgogi|galbi')
                    THEN 'Korean'
                WHEN regexp_matches(LOWER(RestaurantName),
                    'greek|gyro|souvlaki|mediterranean|\bkebab|kabob|falafel|shawarma|hummus|\bpita\b')
                    THEN 'Greek/Mediterranean'
                WHEN regexp_matches(LOWER(RestaurantName),
                    '\bpho\b|vietnamese|banh.?mi|saigon|hanoi')
                    THEN 'Vietnamese'
                WHEN regexp_matches(LOWER(RestaurantName),
                    'caribbean|\bjerk\b|jamaican|\broti\b|plantain|oxtail')
                    THEN 'Caribbean'
                WHEN regexp_matches(LOWER(RestaurantName),
                    'halal|lebanese|turkish|afghan|persian|moroccan')
                    THEN 'Middle Eastern'
                WHEN regexp_matches(LOWER(RestaurantName),
                    'french|bistro|brasserie|creperie|patisserie')
                    THEN 'French'
                ELSE 'Other'
            END AS cuisine
        FROM deduped
        WHERE rn = 1
    """)

    total = con.execute("SELECT COUNT(*) FROM classified_restaurants").fetchone()[0]
    classified = con.execute("SELECT COUNT(*) FROM classified_restaurants WHERE cuisine != 'Other'").fetchone()[0]
    print(f"  {total:,} deduplicated restaurants, {classified:,} classified ({classified*100//total}%)")

    # ---- Nearest station via LATERAL join ----
    print("Computing nearest subway station for each classified restaurant...")
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
        WHERE r.cuisine != 'Other'
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

    print(f"\n  Total classified: {sum(r[1] for r in rows):,}")

    con.close()
    print(f"\nDatabase written to {DB_PATH}")


if __name__ == "__main__":
    download_subway_data()
    build_database()

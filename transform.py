import pandas as pd
import json
import glob
import os

filename = "processed_data/brasileirao.csv"

def transform_data(path_pattern="raw_data/*.json"):
    files = glob.glob(path_pattern)
    all_rows = []

    for file in files:
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)

            events = data.get("events", {}).get("0", [])
            competitors = data.get("competitors", {}).get("0", [])
            champs = data.get("champs", {}).get("0", [])
            odds = data.get("odds", {}).get("0", [])

            comp_map = {c["id"]: c["name"] for c in competitors}
            champ_name = champs[0]["name"] if champs else None


            for ev in events:
                score = ev.get("score", [None, None])
                home_id, away_id = ev.get("competitorIds", [None, None])
                odds_1x2 = [o for o in odds if o.get("typeId") in [1, 2, 3]]

                # principal odds
                try:
                    home_odd = next((o["price"] for o in odds if o["name"] in [comp_map.get(home_id), "1"]), None)
                    draw_odd = next((o["price"] for o in odds if o["name"] in ["Empate", "X"]), None)
                    away_odd = next((o["price"] for o in odds if o["name"] in [comp_map.get(away_id), "2"]), None)
                except:
                    home_odd = draw_odd = away_odd = None

                all_rows.append({
                    "championship": champ_name,
                    "match_id": ev.get("id"),
                    "match_name": ev.get("name"),
                    "home_team": comp_map.get(home_id),
                    "away_team": comp_map.get(away_id),
                    "home_score": score[0],
                    "away_score": score[1],
                    "live_time": ev.get("liveTime"),
                    "status": ev.get("ls"),
                    "start_date": ev.get("startDate"),
                    "home_odd": home_odd,
                    "draw_odd": draw_odd,
                    "away_odd": away_odd,
                    "timestamp_scraped": os.path.basename(file).replace(".json", "")
                })

        except Exception as e:
            print(f"Processing Error {file}: {e}")

    df = pd.DataFrame(all_rows)
    os.makedirs("processed_data", exist_ok=True)
    df = df.drop_duplicates(subset=["match_id", "timestamp_scraped"])
    df.to_csv(filename, index=False, encoding="utf-8-sig", mode='a')
    print(f"✅: {filename} ({len(df)} rows.)")
    return df

if __name__ == "__main__":
    transform_data()

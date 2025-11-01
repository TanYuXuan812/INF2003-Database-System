import pandas as pd
import aiohttp
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

# We need to clean the ratings file by replacing the id from movielens to the tmdb id
# We will clean the links file first to remove rows with null tmdbIds as well

ratings = pd.read_csv("original_data/ratings_small.csv")
links = pd.read_csv("original_data/links_small.csv")
print(ratings.info())

# Check for missing tmdbIds in links
print(f"\nNull tmdbIds in links: {links['tmdbId'].isna().sum()}")

# Remove rows from links where tmdbId is null
links_clean = links[links['tmdbId'].notna()].copy()

# Convert tmdbId to int before merging
links_clean['tmdbId'] = links_clean['tmdbId'].astype(int)

# Now merge
merged = ratings.merge(
    links_clean[["movieId", "tmdbId"]], on="movieId", how="inner")
merged = merged.drop(columns=["movieId"])
merged = merged.rename(columns={"tmdbId": "movieId"})
merged = merged[["userId", "movieId", "rating", "timestamp"]]

print(f"\nOriginal ratings: {len(ratings)}")
print(f"Merged ratings: {len(merged)}")
print(f"Lost ratings: {len(ratings) - len(merged)}")
print("\n", merged.info())

merged.to_csv("updated_data/ratings_small.csv", index=False)

# TMDB_API_KEY = os.getenv("TMDB_API_KEY")
# TMDB_BASE_URL = os.getenv("TMDB_BASE_URL")

# # Limit the number of concurrent requests
# CONCURRENT_REQUESTS = 5
# semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


# async def fetch_poster(session, movie_id):
#     async with semaphore:
#         url = f"{TMDB_BASE_URL}/movie/{movie_id}?api_key={TMDB_API_KEY}"
#         print(f"Fetching poster for movie ID: {movie_id}")
#         try:
#             async with session.get(url) as response:
#                 if response.status == 200:
#                     data = await response.json()
#                     poster_path = data.get("poster_path")
#                     if poster_path:
#                         print(f"Got poster for movie ID: {movie_id}")
#                         # small delay between requests
#                         await asyncio.sleep(0.1)
#                         return f"https://image.tmdb.org/t/p/w500{poster_path}"
#         except Exception as e:
#             print(f"Error fetching {movie_id}: {e}")
#         await asyncio.sleep(0.1)
#         return None


# async def fetch_all_posters(ids):
#     async with aiohttp.ClientSession() as session:
#         tasks = [fetch_poster(session, movie_id) for movie_id in ids]
#         return await asyncio.gather(*tasks)

# # Load CSV
# movies_metadata = pd.read_csv(
#     'original_data/movies_metadata.csv', low_memory=False)

# movie_ids = movies_metadata['id'].tolist()
# poster_urls = asyncio.run(fetch_all_posters(movie_ids))
# movies_metadata['poster_path'] = poster_urls

# # Drop unnecessary columns
# movies_metadata = movies_metadata.drop(columns=[
#     'belongs_to_collection', 'budget', 'homepage', 'imdb_id', 'production_countries', 'spoken_languages', 'status', 'title',
#     'video', 'vote_average', 'vote_count'
# ])

# # Save updated CSV
# movies_metadata.to_csv('updated_data/movies_metadata.csv', index=False)
# print("Done! CSV updated with poster URLs.")

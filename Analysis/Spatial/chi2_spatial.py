import pandas as pd
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import box
from scipy.stats import chi2_contingency
from sqlalchemy import create_engine
import contextily as ctx
import os

# Database
db_connection_str = "postgresql://postgres:********@localhost/postgres"
engine = create_engine(db_connection_str)

# Parameters
term = "volcán"  # Term that will be searched
pixel_size = 75000  # Pixel size in meters

output_folder_png = "png"
output_folder_csv = "csv"
os.makedirs(output_folder_png, exist_ok=True)
os.makedirs(output_folder_csv, exist_ok=True)

# Retrieve Tweets
query_all_tweets = """
SELECT geo_latitude, geo_longitude, tweet_text, found_term
FROM variance.labeled_tweets
WHERE geo_latitude IS NOT NULL 
  AND geo_longitude IS NOT NULL
  AND geo_latitude BETWEEN -56.0 AND 33.0
  AND geo_longitude BETWEEN -117.0 AND -30.0;
"""

# Retrieve relevant Tweets
query_filtered_tweets = f"""
SELECT geo_latitude, geo_longitude, tweet_text, found_term
FROM variance.labeled_tweets
WHERE geo_latitude IS NOT NULL 
  AND geo_longitude IS NOT NULL
  AND geo_latitude BETWEEN -56.0 AND 33.0
  AND geo_longitude BETWEEN -117.0 AND -30.0
  AND found_term = '{term}'
  AND "Label" = 1;
"""

# Fetch all tweets
df_all = pd.read_sql(query_all_tweets, engine)

# Fetch filtered tweets
df_filtered = pd.read_sql(query_filtered_tweets, engine)

# GeoDataFrame
gdf_all = gpd.GeoDataFrame(
    df_all, geometry=gpd.points_from_xy(df_all.geo_longitude, df_all.geo_latitude), crs="EPSG:4326"
)
gdf_all = gdf_all.to_crs("EPSG:3857") 

# GeoDataFrame
gdf_filtered = gpd.GeoDataFrame(
    df_filtered, geometry=gpd.points_from_xy(df_filtered.geo_longitude, df_filtered.geo_latitude), crs="EPSG:4326"
)
gdf_filtered = gdf_filtered.to_crs("EPSG:3857") 

# Bounding box grid
minx, miny, maxx, maxy = gdf_all.total_bounds
grid_cells = []
for x in np.arange(minx, maxx, pixel_size):
    for y in np.arange(miny, maxy, pixel_size):
        grid_cells.append(box(x, y, x + pixel_size, y + pixel_size))

grid = gpd.GeoDataFrame(geometry=grid_cells, crs="EPSG:3857")

# Spatial join
all_tweets_count = gpd.sjoin(gdf_all, grid, how="left", predicate="intersects").groupby('index_right').size()
grid["all_tweets"] = grid.index.map(all_tweets_count).fillna(0)

# Spatial join
filtered_tweets_count = gpd.sjoin(gdf_filtered, grid, how="left", predicate="intersects").groupby('index_right').size()
grid["term_tweets"] = grid.index.map(filtered_tweets_count).fillna(0)

# Expected counts
grid["expected"] = grid["all_tweets"] * (grid["term_tweets"].sum() / grid["all_tweets"].sum())

# Remove rows with 0
valid_grid = grid[(grid["all_tweets"] > 0) & (grid["expected"] > 0)]

# Chi-Square
observed = valid_grid["term_tweets"]
expected = valid_grid["expected"]
valid_grid["residuals"] = (observed - expected) / np.sqrt(expected)

# adjust vmin and vmax
vmin = valid_grid["residuals"].quantile(0.05) 
vmax = valid_grid["residuals"].quantile(0.95) 

# scale symmetric
vmax = max(abs(vmin), abs(vmax))
vmin = -vmax

# hotspots
top_hotspots = valid_grid.sort_values(by="residuals", ascending=False).head(10)
top_hotspots["centroid"] = top_hotspots.geometry.centroid

# WGS84 (EPSG:4326)
top_hotspots["centroid_wgs84"] = top_hotspots["centroid"].to_crs("EPSG:4326")

# latitude and longitude
top_hotspots["latitude"] = top_hotspots["centroid_wgs84"].apply(lambda point: point.y)
top_hotspots["longitude"] = top_hotspots["centroid_wgs84"].apply(lambda point: point.x)

# Hotspot ID
top_hotspots["hotspot_id"] = range(1, len(top_hotspots) + 1)

# CSV
output_csv_path = os.path.join(output_folder_csv, f"top_hotspots_{term}.csv")
top_hotspots[["hotspot_id", "residuals", "latitude", "longitude"]].to_csv(output_csv_path, index=False)
print(f"Top hotspots saved to '{output_csv_path}'.")

# CSV
all_hotspot_tweets = []
for i, row in top_hotspots.iterrows():
    hotspot_geometry = row.geometry
    hotspot_tweets = gdf_filtered[gdf_filtered.geometry.within(hotspot_geometry)]
    if not hotspot_tweets.empty:
        hotspot_tweets["hotspot_id"] = row["hotspot_id"] 
        all_hotspot_tweets.append(hotspot_tweets[["tweet_text", "geo_latitude", "geo_longitude", "hotspot_id"]])

# DataFrame
if all_hotspot_tweets:
    all_tweets_df = pd.concat(all_hotspot_tweets)
    output_all_tweets_path = os.path.join(output_folder_csv, f"all_hotspots_tweets_{term}.csv")
    all_tweets_df.to_csv(output_all_tweets_path, index=False)
    print(f"All hotspot tweets saved to '{output_all_tweets_path}'.")

# Plot
fig, ax = plt.subplots(figsize=(14, 10))

valid_grid.plot(
    column="residuals", 
    cmap="RdBu_r", 
    legend=True, 
    edgecolor="darkgrey", 
    alpha=1, 
    ax=ax, 
    vmin=vmin, 
    vmax=vmax
)

# Add hotspots
for _, row in top_hotspots.iterrows():
    if row["hotspot_id"]: 
        centroid = row["centroid"]
        ax.plot(centroid.x, centroid.y, "o", markersize=9, color="red", alpha=0.6)  
        
                ax.text(
            centroid.x, centroid.y, str(row["hotspot_id"]),
            fontsize=10, ha="center", va="center", color="white",
            bbox=dict(facecolor="darkred", alpha=0.7, edgecolor="none", boxstyle="circle,pad=0.3")  
        )

ctx.add_basemap(ax, crs="EPSG:3857", source=ctx.providers.Esri.WorldGrayCanvas)

ax.set_title(f"Spatial Distribution of Tweets  mentioning '{term}'")
ax.axis("off")

colorbar = ax.get_figure().axes[-1] 
colorbar.set_ylabel("Deviation from expected distribution", fontsize=12)

residuals_map_path = os.path.join(output_folder_png, f"chi2_{term}.png")
plt.savefig(residuals_map_path, dpi=300)
plt.show()

print(f"Residuals map with hotspots and IDs saved as '{residuals_map_path}'.")

# Chi-Square
observed = valid_grid["term_tweets"]
expected = valid_grid["expected"]

# Chi-Square
chi2_stat, p_value, dof, expected_freq = chi2_contingency(
    [observed, expected], correction=False
)

# Filepath
chi2_stats_path = os.path.join(output_folder_csv, f"chi2_stats_{term}.txt")

# Chi-Square
with open(chi2_stats_path, "w") as f:
    f.write("Chi-Square Test Results:\n")
    f.write(f"Chi-Square Statistic: {chi2_stat:.2f}\n")
    f.write(f"Degrees of Freedom: {dof}\n")
    f.write(f"P-Value: {p_value:.4f}\n")
    np.savetxt(f, expected_freq, fmt="%.2f", delimiter=",", header="Expected Frequencies", comments='')

print(f"Chi-Square statistics saved to '{chi2_stats_path}'.")

#script to plot heatpam for all terms based on chi2 analysis

import pandas as pd
import numpy as np
from scipy.stats import chi2_contingency
from sqlalchemy import create_engine
import plotly.express as px

# DB
def connect_to_db():
    try:
        engine = create_engine("postgresql+psycopg2://postgres:************@localhost/postgres")
        return engine
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

engine = connect_to_db()
if engine is None:
    exit()

# Query data
query_all_tweets = """
SELECT EXTRACT(DOY FROM created_at) AS day_of_year, COUNT(*) AS count
FROM variance.labeled_tweets
WHERE "Label" = 1
GROUP BY EXTRACT(DOY FROM created_at);
"""
all_tweets_df = pd.read_sql(query_all_tweets, engine)

query_terms = """
SELECT found_term AS term, EXTRACT(DOY FROM created_at) AS day_of_year, COUNT(*) AS count
FROM variance.labeled_tweets
WHERE "Label" = 1 
AND found_term IN (
    SELECT main_term_sp 
    FROM variance.landscape_terms
    WHERE relevant = true
)
GROUP BY found_term, EXTRACT(DOY FROM created_at);
"""
terms_df = pd.read_sql(query_terms, engine)

engine.dispose()

# pivot data
daily_counts = terms_df.pivot(index='term', columns='day_of_year', values='count').fillna(0)

# All days
daily_counts = daily_counts.reindex(columns=range(1, 366), fill_value=0)

# Pivot data
all_tweets_daily = all_tweets_df.set_index("day_of_year").reindex(range(1, 366), fill_value=0)["count"]

# Remove 0
observed = daily_counts.loc[daily_counts.sum(axis=1) > 0, daily_counts.sum(axis=0) > 0]

# Calculate total 
total_tweets_per_day = all_tweets_daily.reindex(observed.columns) 
total_tweets_per_term = observed.sum(axis=1)
total_tweets = observed.values.sum()

# Check for zero
if total_tweets == 0 or (total_tweets_per_day == 0).any() or (total_tweets_per_term == 0).any():
    raise ValueError("Zero totals detected in rows or columns, cannot compute chi-squared test.")

# Compute expected frequencies 
expected = np.outer(total_tweets_per_term, total_tweets_per_day) / total_tweets

# Add a small constant
expected[expected == 0] = 1e-10

# chi2
chi2, p, dof, expected = chi2_contingency(observed)

# Calculate normalized residuals
normalized_residuals = (observed - expected) / np.sqrt(expected)

# Create month
month_days = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
month_labels = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", 
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]

# Plot the heatmap
fig = px.imshow(
    normalized_residuals.values,
    labels=dict(x="Day of Year", y="Landscape Term", color="Normalized Residual"),
    x=observed.columns,  
    y=observed.index,  
    color_continuous_scale="RdBu_r",
    zmin=-3, zmax=3
)

# Add month labels
fig.update_xaxes(
    tickvals=month_days,
    ticktext=month_labels,
    title="Month"
)

fig.update_layout(
    title=dict(
        text="Daily Anomalies: Each Term Compared to All Tweets",
        x=0.5,  
        font=dict(size=16) 
    ),
    yaxis_title="Landscape Term",
    coloraxis_colorbar=dict(
        title="",
        tickvals=[-3, -2, -1, 0, 1, 2, 3],
        ticktext=["Strong Negative", "Negative", "Slight Negative", "Normal", "Slight Positive", "Positive", "Strong Positive"]
    )
)


fig.show()
fig.write_image("overview.png", width=1920, height=1080, scale=3)


all_terms_pvalues = []

for term in observed.index:
    observed_term = observed.loc[term].values
    expected_term = expected[observed.index.get_loc(term)]
    
    chi2_term, p_term = chi2_contingency([observed_term, expected_term])[:2]
    
    all_terms_pvalues.append((term, f"{p_term:.6f}"))  

# DF
all_terms_pvalues_df = pd.DataFrame(all_terms_pvalues, columns=["Term", "P-Value"])

# Sort terms by p-value
all_terms_pvalues_df = all_terms_pvalues_df.sort_values(by="P-Value", key=lambda x: x.astype(float))

# Display the p-values
print("P-Values for All Terms:")
print(all_terms_pvalues_df)

# Save to CSV
all_terms_pvalues_df.to_csv("all_terms_pvalues.csv", index=False)

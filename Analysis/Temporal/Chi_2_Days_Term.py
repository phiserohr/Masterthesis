#script to plot heatpam for indivuiaul terms based on chi2 analysis
#also prints the days with the highest deviation
#change term at the bottom
import pandas as pd
import numpy as np
from scipy.stats import chi2_contingency
from sqlalchemy import create_engine
import plotly.express as px

# DB
def connect_to_db():
    try:
        engine = create_engine("postgresql+psycopg2://postgres:***********@localhost/postgres")
        return engine
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

def plot_term_residuals(term):
    engine = connect_to_db()
    if engine is None:
        exit()

    # Query data
    query_term = f"""
    SELECT EXTRACT(DOY FROM created_at) AS day_of_year, COUNT(*) AS count
    FROM variance.labeled_tweets
    WHERE "Label" = 1 AND found_term = '{term}'
    GROUP BY EXTRACT(DOY FROM created_at);
    """
    term_df = pd.read_sql(query_term, engine)

    query_all_tweets = """
    SELECT EXTRACT(DOY FROM created_at) AS day_of_year, COUNT(*) AS count
    FROM variance.labeled_tweets
    WHERE "Label" = 1
    GROUP BY EXTRACT(DOY FROM created_at);
    """
    all_tweets_df = pd.read_sql(query_all_tweets, engine)

    engine.dispose()

    # DF MErge
    merged_df = pd.merge(
        term_df, all_tweets_df, on="day_of_year", suffixes=(f"_{term}", "_all"), how="right"
    ).fillna(0)

    observed = np.array([merged_df[f"count_{term}"], merged_df["count_all"] - merged_df[f"count_{term}"]])

    # Expected Counts
    total_term = observed[0].sum()
    total_other = observed[1].sum()
    total_tweets_per_day = observed.sum(axis=0)

    expected = np.array([
        total_term * total_tweets_per_day / total_tweets_per_day.sum(),
        total_other * total_tweets_per_day / total_tweets_per_day.sum()
    ])

    # Chi2
    chi2, p, dof, expected = chi2_contingency(observed)

    # normalized residuals
    normalized_residuals = (observed - expected) / np.sqrt(expected)

    # Add residuals to the dataframe for visualization
    merged_df["normalized_residual"] = normalized_residuals[0]

    # months
    month_days = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
    month_labels = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", 
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    ]

    # Plot
    fig = px.bar(
        merged_df,
        x="day_of_year",
        y="normalized_residual",
        labels={"day_of_year": "Month", "normalized_residual": "Normalized Residual"},
        title=f"Anomalies for Tweets Containing '{term}'",
        color="normalized_residual",
        color_continuous_scale="RdBu_r"
    )

    # Month
    fig.update_xaxes(
        tickvals=month_days,
        ticktext=month_labels,
        title="Month",
        tickfont=dict(size=14)  # Increased font size
    )

    # Gridlines
    fig.update_yaxes(
        title="Normalized Residual",
        tickfont=dict(size=16),  # Increased font size
        title_font=dict(size=18),  # Larger axis title font
        showgrid=True,
        gridcolor="lightgrey",
        gridwidth=0.5
    )

    # PNG-Export
    fig.update_layout(
        title=dict(
            text=f"Daily Anomalies: '{term}' Compared to All Tweets",
            font=dict(size=22), 
            x=0.5, 
        ),
        xaxis=dict(title_font=dict(size=18)), 
        coloraxis_colorbar=dict(
            title="Residual",
            tickvals=[-5, -2.5, 0, 2.5, 5],
            ticktext=["S-", "M-", "N", "M+", "S+"],
            title_font=dict(size=16),  
            tickfont=dict(size=14) 
        ),
        coloraxis=dict(cmin=-5, cmax=5), 
        margin=dict(l=60, r=60, t=80, b=60),  
        height=600,  
        width=1000 
    )
    fig.show()

    # Save Plot
    fig.write_image("montaña.png", width=1600, height=1000, scale=3)

    # Print 5 days
    print(f"Top 5 Days for '{term}':")
    print(merged_df.nlargest(5, f"count_{term}")[["day_of_year", f"count_{term}"]])

plot_term_residuals("montaña") 



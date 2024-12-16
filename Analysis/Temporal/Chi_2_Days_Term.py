import pandas as pd
import numpy as np
from scipy.stats import chi2_contingency
from sqlalchemy import create_engine
import plotly.express as px

# Create a connection to the PostgreSQL database using SQLAlchemy
def connect_to_db():
    try:
        engine = create_engine("postgresql+psycopg2://postgres:NordProJect_123@localhost/postgres")
        return engine
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

def plot_term_residuals(term):
    # Connect to the PostgreSQL database
    engine = connect_to_db()
    if engine is None:
        exit()

    # Query data for relevant tweets from the variance.labeled_tweets table
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

    # Close the engine connection
    engine.dispose()

    # Merge the dataframes to align tweet counts by day
    merged_df = pd.merge(
        term_df, all_tweets_df, on="day_of_year", suffixes=(f"_{term}", "_all"), how="right"
    ).fillna(0)

    # Prepare the contingency table for each day
    observed = np.array([merged_df[f"count_{term}"], merged_df["count_all"] - merged_df[f"count_{term}"]])

    # Calculate expected counts assuming independence
    total_term = observed[0].sum()
    total_other = observed[1].sum()
    total_tweets_per_day = observed.sum(axis=0)

    expected = np.array([
        total_term * total_tweets_per_day / total_tweets_per_day.sum(),
        total_other * total_tweets_per_day / total_tweets_per_day.sum()
    ])

    # Perform chi-squared test
    chi2, p, dof, expected = chi2_contingency(observed)

    # Calculate normalized residuals
    normalized_residuals = (observed - expected) / np.sqrt(expected)

    # Add residuals to the dataframe for visualization
    merged_df["normalized_residual"] = normalized_residuals[0]

    # Create month labels for the x-axis
    month_days = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
    month_labels = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", 
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    ]

    # Plot the residuals with a fixed scale
    fig = px.bar(
        merged_df,
        x="day_of_year",
        y="normalized_residual",
        labels={"day_of_year": "Month", "normalized_residual": "Normalized Residual"},
        title=f"Anomalies for Tweets Containing '{term}'",
        color="normalized_residual",
        color_continuous_scale="RdBu_r"
    )

    # Update the x-axis with month labels
    fig.update_xaxes(
        tickvals=month_days,
        ticktext=month_labels,
        title="Month",
        tickfont=dict(size=14)  # Increased font size
    )

    # Add gridlines for better readability
    fig.update_yaxes(
        title="Normalized Residual",
        tickfont=dict(size=16),  # Increased font size
        title_font=dict(size=18),  # Larger axis title font
        showgrid=True,
        gridcolor="lightgrey",
        gridwidth=0.5
    )

    # Update layout for better readability and PNG export
    fig.update_layout(
        title=dict(
            text=f"Daily Anomalies: '{term}' Compared to All Tweets",
            font=dict(size=22),  # Larger title font
            x=0.5,  # Center the title
        ),
        xaxis=dict(title_font=dict(size=18)),  # Larger x-axis title font
        coloraxis_colorbar=dict(
            title="Residual",
            tickvals=[-5, -2.5, 0, 2.5, 5],
            ticktext=["S-", "M-", "N", "M+", "S+"],
            title_font=dict(size=16),  # Larger colorbar title font
            tickfont=dict(size=14)  # Larger colorbar tick font
        ),
        coloraxis=dict(cmin=-5, cmax=5),  # Limit the range to -5 to 5
        margin=dict(l=60, r=60, t=80, b=60),  # Adjust margins to give space for labels
        height=600,  # Larger height for better readability
        width=1000  # Larger width for document embedding
    )

    # Show the plot
    fig.show()

    # Save the plot as a high-resolution PNG
    fig.write_image("montaña.png", width=1600, height=1000, scale=3)

    # Print the top 5 days with the highest counts for the term
    print(f"Top 5 Days for '{term}':")
    print(merged_df.nlargest(5, f"count_{term}")[["day_of_year", f"count_{term}"]])

# Call the function with the term you want to analyze
plot_term_residuals("montaña")  # Replace "lago" with any other term



# Import python packages
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as F
from snowflake.snowpark.window import Window
session = get_active_session()


df_amazon = session.table("hulu_MOVIES_TV")
df_disney = session.table("disney_MOVIES_TV")
df_hulu = session.table("HULU_MOVIES_TV")
df_netflix = session.table("netflix_MOVIES_TV")

df_industry = session.table("INDUSTRY_MOVIES")
df_IMDB = session.table("IMDB_MOVIE_REVIEWS")

df_streaming_filled = df_streaming
df_streaming = df_streaming.select("director").join(df_amazon.select("director"), "director", "left")

# Count duplicate movie_id in df_streaming
duplicate_count = df_streaming.group_by("movie_id").count().filter(F.col("count") > 1).count()
st.write(f"Number of duplicate movie_id in df_streaming: {duplicate_count}")

# Delete rows with duplicate MOVIES_KEY
window_spec = Window.partition_by("MOVIES_KEY").order_by(F.col("MOVIES_KEY"))
df_streaming = df_streaming.with_column("row_number", F.row_number().over(window_spec)).filter(F.col("row_number") == 1).drop("row_number")

import streamlit as st
import awswrangler as wr
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import scipy.interpolate as interp

@st.cache_data
def get_profile_stats():
    data = wr.athena.read_sql_query(
        sql = """
            SELECT
                profile as "profile",
                COUNT(id) as "videos detected",
                
                CAST(SUM(duration) as BIGINT) as "total duration",
                CAST(SUM(view_count) as BIGINT) as "total views",
                CAST(SUM(like_count) as BIGINT) as "total likes",
                CAST(SUM(comment_count) as BIGINT) as "total comments",
                CAST(SUM(repost_count) as BIGINT) as "total reposts",
                
                CAST(AVG(duration) as DOUBLE) as "average duration",
                CAST(AVG(view_count) as DOUBLE) as "average views",
                CAST(AVG(like_count) as DOUBLE) as "average likes",
                CAST(AVG(comment_count) as DOUBLE) as "average comments",
                CAST(AVG(repost_count) as DOUBLE) as "average reposts",
                
                CAST(MIN(duration) as BIGINT) as "minimum duration",
                CAST(MIN(view_count) as BIGINT) as "minimum views",
                CAST(MIN(like_count) as BIGINT) as "minimum likes",
                CAST(MIN(comment_count) as BIGINT) as "minimum comments",
                CAST(MIN(repost_count) as BIGINT) as "minimum reposts",
                
                CAST(MAX(duration) as BIGINT) as "maximum duration",
                CAST(MAX(view_count) as BIGINT) as "maximum views",
                CAST(MAX(like_count) as BIGINT) as "maximum likes",
                CAST(MAX(comment_count) as BIGINT) as "maximum comments",
                CAST(MAX(repost_count) as BIGINT) as "maximum reposts"
            FROM metadata
            GROUP BY profile
        """,
        database = "tiktok_analytics"
    )
    
    quality = data[["profile", "videos detected"]]
    duration = data[["profile", "total duration", "average duration", "minimum duration", "maximum duration"]]
    views = data[["profile", "total views", "average views", "minimum views", "maximum views"]]
    likes = data[["profile", "total likes", "average likes", "minimum likes", "maximum likes"]]
    comments = data[["profile", "total comments", "average comments", "minimum comments", "maximum comments"]]
    reposts = data[["profile", "total reposts", "average reposts", "minimum reposts", "maximum reposts"]]
    
    quality.columns = quality.columns.str.title()
    duration.columns = duration.columns.str.title()
    views.columns = views.columns.str.title()
    likes.columns = likes.columns.str.title()
    comments.columns = comments.columns.str.title()
    reposts.columns = reposts.columns.str.title()
    
    stats = {
        "quality": quality,
        "duration": duration,
        "views": views,
        "likes": likes,
        "comments": comments,
        "reposts": reposts
    }
    
    return stats

@st.cache_data
def get_profile_videos_analyzed():
    data = wr.athena.read_sql_query(
        sql = """
            SELECT
                profile as "profile",
                COUNT(*) as "videos analyzed"
            FROM text_analysis
            GROUP BY profile
        """,
        database = "tiktok_analytics"
    )
    
    data.columns = data.columns.str.title()
    
    return data

@st.cache_data
def get_all_profiles_data():
    data = wr.athena.read_sql_query(
        sql = """
            SELECT
                id,
                profile,
                upload_date as "upload date",
                duration as "duration",
                view_count as "views",
                like_count as "likes",
                comment_count as "comments",
                repost_count as "reposts"
            FROM metadata
            ORDER BY upload_date
        """,
        database = "tiktok_analytics"
    )
    
@st.cache_data
def get_profile_metadata(profile):
    data = wr.athena.read_sql_query(
        sql = f"""
            SELECT
                id,
                upload_date as "upload date",
                duration as "duration",
                view_count as "views",
                like_count as "likes",
                comment_count as "comments",
                repost_count as "reposts"
            FROM metadata
            WHERE profile = '{profile}'
            ORDER BY upload_date
        """,
        database = "tiktok_analytics"
    )
    
    data.columns = data.columns.str.title()
    data = data.rename(columns = {'Id': 'Video ID'})
    
    data['Upload Date'] = pd.to_datetime(data['Upload Date'])
    
    return data

stats = get_profile_stats()
videos_analyzed = get_profile_videos_analyzed()

quality = stats["quality"]
quality = quality.merge(videos_analyzed, on = "Profile", how = "left")
quality['Videos Analyzed'] = quality['Videos Analyzed'].fillna(0)

profiles = quality['Profile'].unique().tolist()

st.write('# All Profiles')
col1, col2 = st.columns(2)

with col1:
    st.write("### Views")
    st.dataframe(
        stats["views"].set_index("Profile"),
        use_container_width = True
    )
    
    st.write("### Comments")
    st.dataframe(
        stats["comments"].set_index("Profile"),
        use_container_width = True
    )
    
    st.write("### Duration")
    st.dataframe(
        stats["duration"].set_index("Profile"),
        use_container_width = True
    )

with col2:
    st.write("### Likes") 
    st.dataframe(
        stats["likes"].set_index("Profile"),
        use_container_width = True
    )
    
    st.write("### Reposts")
    st.dataframe(
        stats["reposts"].set_index("Profile"),
        use_container_width = True
    )
    
    st.write("### Data Quality")
    st.dataframe(
        quality.set_index("Profile"),
        use_container_width = True
    )

st.write('# Profile Level Data')

profile = st.selectbox('Select a profile', profiles)

metadata = get_profile_metadata(profile)

# Create time series plot with toggles
metrics = ["Views", "Likes", "Comments", "Reposts", "Duration"]

# Initialize or reset session state if metrics change
if 'metric_visibility' not in st.session_state or len(st.session_state.metric_visibility) != len(metrics):
    st.session_state.metric_visibility = {metric: True for metric in metrics}
# Add any missing metrics with default value True
for metric in metrics:
    if metric not in st.session_state.metric_visibility:
        st.session_state.metric_visibility[metric] = True

# First time series section with toggles
col1, col2 = st.columns([5, 1])

with col2:
    st.write("#### Toggle Metrics")  # Empty space for alignment
    for _ in range(5):  # Adjust number for alignment
        st.write("")
    
    for metric in metrics:
        st.write("")  # Space between toggles
        if st.checkbox(
            metric, 
            value = st.session_state.metric_visibility[metric],
            key = f"toggle_{metric}"
        ):
            st.session_state.metric_visibility[metric] = True
        else:
            st.session_state.metric_visibility[metric] = False

with col1:
    fig = go.Figure()

    for metric in metrics:
        fig.add_trace(go.Scatter(
            x = metadata["Upload Date"],
            y = metadata[metric],
            name = metric,
            mode = 'markers',
            visible = st.session_state.metric_visibility[metric]
        ))

    fig.update_layout(
        title = "Video Metrics Over Time",
        xaxis_title = "Upload Date",
        yaxis_title = "Count",
        height = 500,
        template = "plotly_dark",
        showlegend = True,
        legend = dict(
            yanchor = "top",
            y = 0.99,
            xanchor = "right",
            x = 0.99,
            bgcolor = "rgba(0,0,0,0.5)"
        ),
        yaxis = dict(
            type = 'log',
            rangemode = 'tozero',
            dtick = 1,
            ticktext = ['10<sup>' + str(i) + '</sup>' for i in range(-3, 9)],
            tickvals = [10**i for i in range(-3, 9)],
            tickmode = 'array',
            tickformat = None
        )
    )

    st.plotly_chart(fig, use_container_width=True, key="time_series_main")

# First ratio section - Time series
st.write("")  # Add space between sections
ratio_ts_col1, ratio_ts_col2 = st.columns([5, 1])

with ratio_ts_col2:
    st.write("#### Time Series Ratio")
    
    for _ in range(6):  # Adjust number for alignment
        st.write("")
    
    ts_numerator = st.selectbox(
        "Select numerator metric",
        options=metrics,
        key="ts_ratio_numerator"
    )
    
    st.write("")
    st.write("")
    
    ts_denominator = st.selectbox(
        "Select denominator metric", 
        options=metrics,
        key="ts_ratio_denominator"
    )

with ratio_ts_col1:
    # Time series ratio plot
    ratio = metadata[ts_numerator] / metadata[ts_denominator]
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x = metadata["Upload Date"],
        y = ratio,
        mode = 'markers',
        name = f'{ts_numerator}/{ts_denominator} Ratio'
    ))
    
    fig.update_layout(
        title = f"{ts_numerator}/{ts_denominator} Ratio Over Time",
        xaxis_title = "Upload Date",
        yaxis_title = "Ratio",
        height = 500,
        template = "plotly_dark",
        showlegend = True,
        legend = dict(
            yanchor = "top",
            y = 0.99,
            xanchor = "right",
            x = 0.99,
            bgcolor = "rgba(0,0,0,0.5)"
        ),
        yaxis = dict(
            type = 'log',
            rangemode = 'tozero',
            dtick = 1,
            ticktext = ['10<sup>' + str(i) + '</sup>' for i in range(-3, 3)],
            tickvals = [10**i for i in range(-3, 3)],
            tickmode = 'array',
            tickformat = None
        )
    )
    
    st.plotly_chart(fig, use_container_width=True, key=f"ratio_ts_{ts_numerator}_{ts_denominator}")

# Second ratio section - Scatter plots
st.write("")  # Add space between sections
scatter_col1, scatter_col2, scatter_col3 = st.columns([4, 4, 1.5])  # Adjusted ratios

with scatter_col3:
    st.write("#### Correlations")  # Shorter title that won't wrap
    
    for _ in range(6):  # Adjust number for alignment
        st.write("")
    
    metric_a = st.selectbox(
        "Select first metric",
        options=metrics,
        key="scatter_a"
    )
    
    st.write("")
    st.write("")
    
    metric_b = st.selectbox(
        "Select second metric", 
        options=metrics,
        key="scatter_b"
    )

# Function to create scatter plot with spline
def create_scatter_plot(x_metric, y_metric, plot_id):
    fig = go.Figure()
    
    # Add scatter points
    fig.add_trace(go.Scatter(
        x = metadata[x_metric],
        y = metadata[y_metric],
        mode = 'markers',
        name = 'Data Points',
        marker = dict(size=6)
    ))
    
    # Add spline fit
    try:
        # Filter out zeros and negative values for log space
        valid_data = (metadata[x_metric] > 0) & (metadata[y_metric] > 0)
        x_valid = metadata[x_metric][valid_data]
        y_valid = metadata[y_metric][valid_data]
        
        if len(x_valid) > 3:  # Only fit if we have enough valid points
            sorted_indices = x_valid.argsort()
            x_sorted = x_valid.iloc[sorted_indices]
            y_sorted = y_valid.iloc[sorted_indices]
            
            # Handle log space calculations safely
            x_log = np.log10(x_sorted)
            y_log = np.log10(y_sorted)
            
            spl = interp.UnivariateSpline(x_log, y_log, s=len(x_sorted)/10)
            
            x_smooth = np.logspace(x_log.min(), x_log.max(), 200)
            y_smooth = 10**spl(np.log10(x_smooth))
            
            fig.add_trace(go.Scatter(
                x = x_smooth,
                y = y_smooth,
                mode = 'lines',
                name = 'Trend Line',
                line = dict(color='red', width=2)
            ))
    except Exception as e:
        st.warning(f"Could not fit trend line: {str(e)}")
    
    fig.update_layout(
        title = f"{y_metric} vs {x_metric}",
        xaxis_title = x_metric,
        yaxis_title = y_metric,
        height = 500,
        template = "plotly_dark",
        showlegend = True,
        legend = dict(
            yanchor = "top",
            y = 0.99,
            xanchor = "right",
            x = 0.99,
            bgcolor = "rgba(0,0,0,0.5)"
        ),
        xaxis = dict(
            type = 'log',
            rangemode = 'tozero',
            dtick = 1,
            ticktext = ['10<sup>' + str(i) + '</sup>' for i in range(-3, 9)],
            tickvals = [10**i for i in range(-3, 9)],
            tickmode = 'array',
            tickformat = None
        ),
        yaxis = dict(
            type = 'log',
            rangemode = 'tozero',
            dtick = 1,
            ticktext = ['10<sup>' + str(i) + '</sup>' for i in range(-3, 9)],
            tickvals = [10**i for i in range(-3, 9)],
            tickmode = 'array',
            tickformat = None
        )
    )
    return fig

with scatter_col1:
    fig = create_scatter_plot(metric_a, metric_b, "left")
    st.plotly_chart(fig, use_container_width=True, key=f"scatter_left_{metric_a}_{metric_b}")

with scatter_col2:
    fig = create_scatter_plot(metric_b, metric_a, "right")
    st.plotly_chart(fig, use_container_width=True, key=f"scatter_right_{metric_b}_{metric_a}")

cols = st.columns(5)

with cols[0]:
    fig = px.histogram(metadata, x="Views", title="Views")
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True, key="hist_views")

with cols[1]:
    fig = px.histogram(metadata, x="Likes", title="Likes") 
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True, key="hist_likes")

with cols[2]:
    fig = px.histogram(metadata, x="Comments", title="Comments")
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True, key="hist_comments")

with cols[3]:
    fig = px.histogram(metadata, x="Reposts", title="Reposts")
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True, key="hist_reposts")

with cols[4]:
    fig = px.histogram(metadata, x="Duration", title="Duration")
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True, key="hist_duration")

st.dataframe(metadata, use_container_width = True)
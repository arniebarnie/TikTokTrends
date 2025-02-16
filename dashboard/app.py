import streamlit as st
import boto3
import awswrangler as wr
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import scipy.interpolate as interp
import scipy.stats
from scipy import sparse
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.linear_model import ElasticNetCV
from plotly.subplots import make_subplots

# Set up AWS credentials from secrets
aws_credentials = st.secrets["aws_credentials"]

# Create AWS session with role assumption
session = boto3.Session(
    aws_access_key_id = aws_credentials["aws_access_key_id"],
    aws_secret_access_key = aws_credentials["aws_secret_access_key"],
    region_name = aws_credentials["region"]
)

# Create STS client to assume role
sts_client = session.client('sts')
assumed_role_object = sts_client.assume_role(
    RoleArn = aws_credentials["role_arn"],
    RoleSessionName = "StreamlitSession"
)

# Create session with temporary credentials
role_session = boto3.Session(
    aws_access_key_id=assumed_role_object['Credentials']['AccessKeyId'],
    aws_secret_access_key=assumed_role_object['Credentials']['SecretAccessKey'],
    aws_session_token=assumed_role_object['Credentials']['SessionToken'],
    region_name=aws_credentials["region"]
)

# Update wrangler config to use role session
wr.config.boto3_session = role_session

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
                metadata.profile as "profile",
                metadata.upload_date as "upload date",
                metadata.duration as "duration",
                metadata.view_count as "views",
                metadata.like_count as "likes",
                metadata.comment_count as "comments",
                metadata.repost_count as "reposts",
                text_analysis.language as "language",
                text_analysis.category as "category",
                text_analysis.summary as "summary",
                text_analysis.keywords as "keywords"
            FROM metadata
            INNER JOIN text_analysis USING (id)
            ORDER BY upload_date
        """,
        database = "tiktok_analytics"
    )
    
    data.columns = data.columns.str.title()
    data['Upload Date'] = pd.to_datetime(data['Upload Date'])
    data = data.rename(columns = {'Id': 'Video ID'})
    
    # Capitalize category names (handling NA values)
    data['Category'] = data['Category'].fillna('Unknown').apply(
        lambda x: '/'.join(word.capitalize() for word in x.split('/'))
    )
    
    # Remove Unknown/NA categories
    data = data[data['Category'] != 'Unknown']
    
    return data

@st.cache_data
def keyword_model_for_category(category):
    # Filter data for category and ensure Keywords is a list
    data = all_profiles_data[all_profiles_data['Category'] == category].copy()
    data = data.sample(min(1000, len(data)))
    data['Keywords'] = data['Keywords'].apply(lambda x: eval(x) if isinstance(x, str) else x)
    
    # Ensure numeric data types
    data['Views'] = pd.to_numeric(data['Views'], errors='coerce')
    data['Duration'] = pd.to_numeric(data['Duration'], errors='coerce')
    
    # Remove any rows with NA values
    data = data.dropna(subset=['Views', 'Duration'])
    
    # Create feature matrices
    profile_dummies = pd.get_dummies(data['Profile'], dtype=float)
    
    # Handle keywords properly
    mlb = MultiLabelBinarizer(sparse_output=True)
    keywords_dummies = mlb.fit_transform([kw for kw in data['Keywords']])
    
    # Convert to sparse matrices with explicit dtype
    duration_sparse = sparse.csr_matrix(data[['Duration']].astype(float).values)
    profile_sparse = sparse.csr_matrix(profile_dummies.values)
    
    # Combine features
    X = sparse.hstack([duration_sparse, profile_sparse, keywords_dummies])
    y = data['Views'].values
    
    # Fit model
    model = ElasticNetCV(
        cv = 5,
        random_state = 0,
        max_iter = 10000,
        l1_ratio = [.1, .5, .7, .9, .95, .99, 1]  # Test different L1 ratios
    )
    model.fit(X, y)
    
    # Get feature importance
    keyword_importance = pd.DataFrame({
        'Keyword': mlb.classes_,
        'Impact': model.coef_[-len(mlb.classes_) :]
    })
    
    return keyword_importance

@st.cache_data
def top_predictive_keywords_for_category(category):
    keyword_importances = keyword_model_for_category(category)
    head = keyword_importances.sort_values(by = 'Impact', key = abs, ascending = False).head(10)
    return head[head['Impact'] > 0]

st.title("TikTok's Top Influencers")

stats = get_profile_stats()
videos_analyzed = get_profile_videos_analyzed()

quality = stats["quality"]
quality = quality.merge(videos_analyzed, on = "Profile", how = "left")
quality['Videos Analyzed'] = quality['Videos Analyzed'].fillna(0)

profiles = quality['Profile'].unique().tolist()

st.write('# Video Metrics')
col1, col2 = st.columns(2)

with col1:
    with st.expander("### Views", expanded=False):
        st.dataframe(
            stats["views"].set_index("Profile"),
            use_container_width=True
        )
    
    with st.expander("### Comments", expanded=False):
        st.dataframe(
            stats["comments"].set_index("Profile"),
            use_container_width=True
        )
    
    with st.expander("### Duration", expanded=False):
        st.dataframe(
            stats["duration"].set_index("Profile"),
            use_container_width=True
        )

with col2:
    with st.expander("### Likes", expanded=False):
        st.dataframe(
            stats["likes"].set_index("Profile"),
            use_container_width=True
        )
    
    with st.expander("### Reposts", expanded=False):
        st.dataframe(
            stats["reposts"].set_index("Profile"),
            use_container_width=True
        )
    
    with st.expander("### Data Quality", expanded=False):
        st.dataframe(
            quality.set_index("Profile"),
            use_container_width=True
        )

all_profiles_data = get_all_profiles_data()

# Histograms row
with st.expander("How are the video metrics distributed?", expanded=False):
    hist_cols = st.columns(5)
    
    with hist_cols[0]:
        fig_views = px.histogram(
            all_profiles_data, 
            x = "Views", 
            title = "Views Distribution",
            log_x = False
        )
        fig_views.update_layout(
            showlegend=False,
            title={
                'text': "Views Distribution",
                'x': 0.5,
                'xanchor': 'center'
            }
        )
        st.plotly_chart(fig_views, use_container_width = True)
    
    with hist_cols[1]:
        fig_likes = px.histogram(
            all_profiles_data, 
            x = "Likes", 
            title = "Likes Distribution",
            log_x = False
        )
        fig_likes.update_layout(showlegend = False)
        st.plotly_chart(fig_likes, use_container_width = True)
    
    with hist_cols[2]:
        fig_comments = px.histogram(
            all_profiles_data, 
            x = "Comments", 
            title = "Comments Distribution",
            log_x = False
        )
        fig_comments.update_layout(showlegend = False)
        st.plotly_chart(fig_comments, use_container_width = True)
    
    with hist_cols[3]:
        fig_reposts = px.histogram(
            all_profiles_data, 
            x = "Reposts", 
            title = "Reposts Distribution",
            log_x = False
        )
        fig_reposts.update_layout(showlegend = False)
        st.plotly_chart(fig_reposts, use_container_width = True)
    
    with hist_cols[4]:
        fig_duration = px.histogram(
            all_profiles_data, 
            x = "Duration", 
            title = "Duration Distribution"
        )
        fig_duration.update_layout(showlegend = False)
        st.plotly_chart(fig_duration, use_container_width = True)

# Time series section
with st.expander("How have these metrics changed over time?", expanded=False):
    # Time series plot
    ts_col1, ts_col2 = st.columns([3, 1])
    
    # Define available metrics and their default states
    metrics = {
        "Views": True,
        "Likes": False,
        "Comments": False,
        "Reposts": False,
        "Duration": False
    }
    
    # Checkbox column
    with ts_col2:
        st.write("Select Metrics")
        metric_toggles = {
            metric: st.checkbox(metric, value = default_state)
            for metric, default_state in metrics.items()
        }

    # Plot column
    with ts_col1:
        # Create figure
        fig = go.Figure()
        
        # Add a trace for each selected metric
        for metric, is_selected in metric_toggles.items():
            if is_selected:
                # Calculate daily averages
                daily_avg = all_profiles_data.groupby('Upload Date')[metric].mean()
                
                # Add trace
                fig.add_trace(
                    go.Scatter(
                        x = daily_avg.index,
                        y = daily_avg.values,
                        name = metric,
                        mode = 'markers',
                        marker = dict(size = 4)
                    )
                )
        
        # Update layout
        fig.update_layout(
            title={
                'text': "Metrics Over Time",
                'x': 0.5,
                'xanchor': 'center'
            },
            xaxis_title="Date",
            yaxis_title="Value",
            height=400,
            showlegend=True,
            yaxis_type="log"
        )
        
        st.plotly_chart(fig, use_container_width = True)

    # Add a separator
    st.write("---")
    
    # Ratio plot section
    ratio_col1, ratio_col2 = st.columns([3, 1])
    
    # Metric selection column
    with ratio_col2:
        st.write("Select Metrics for Ratio")
        numerator = st.selectbox(
            "Numerator",
            options=["Views", "Likes", "Comments", "Reposts", "Duration"],
            index=1
        )
        denominator = st.selectbox(
            "Denominator",
            options=["Views", "Likes", "Comments", "Reposts", "Duration"],
            index=0
        )

    # Plot column
    with ratio_col1:
        if numerator != denominator:
            # Calculate ratio for each video
            ratio_data = pd.DataFrame({
                'Date': all_profiles_data['Upload Date'],
                'Ratio': all_profiles_data[numerator] / all_profiles_data[denominator]
            })
            
            # Create figure
            fig = go.Figure()
            
            fig.add_trace(
                go.Scatter(
                    x = ratio_data['Date'],
                    y = ratio_data['Ratio'],
                    mode = 'markers',
                    marker = dict(size = 4),
                    name = f'{numerator}/{denominator} Ratio'
                )
            )
            
            # Update layout
            fig.update_layout(
                title={
                    'text': f"{numerator} to {denominator} Ratio",
                    'x': 0.5,
                    'xanchor': 'center'
                },
                xaxis_title="Date",
                yaxis_title=f"{numerator}/{denominator}",
                height=400,
                showlegend=False,
                yaxis_type="log"
            )
            
            st.plotly_chart(fig, use_container_width = True)
        else:
            st.warning("Please select different metrics for the ratio calculation")

# Correlation section
with st.expander("What is their relationship?", expanded=False):
    # Create layout
    corr_col1, corr_col2, corr_col3 = st.columns([2, 2, 1])
    
    # Metric selection column
    with corr_col3:
        st.write("Select Metrics")
        metric_a = st.selectbox(
            "Metric A",
            options=["Views", "Likes", "Comments", "Reposts", "Duration"],
            index=0,
            key="corr_metric_a"
        )
        metric_b = st.selectbox(
            "Metric B",
            options=["Views", "Likes", "Comments", "Reposts", "Duration"],
            index=1,
            key="corr_metric_b"
        )
        
        if metric_a != metric_b:
            # Calculate correlations
            pearson_corr = all_profiles_data[metric_a].corr(all_profiles_data[metric_b], method='pearson')
            spearman_corr = all_profiles_data[metric_a].corr(all_profiles_data[metric_b], method='spearman')
            
            st.write("---")
            st.write("Correlations")
            st.write(f"**Pearson:** {pearson_corr:.3f}")
            st.write(f"**Spearman:** {spearman_corr:.3f}")

    if metric_a != metric_b:
        # A vs B plot
        with corr_col1:
            # Prepare data for spline fitting
            ab_data = all_profiles_data[[metric_a, metric_b]].copy()
            ab_data = ab_data[ab_data[[metric_a, metric_b]].gt(0).all(axis=1)]  # Remove zeros and negatives
            ab_data = ab_data.sort_values(metric_a)
            ab_data = ab_data.groupby(metric_a)[metric_b].mean().reset_index()  # Handle duplicates
            
            # Fit spline with higher smoothing factor
            spl = interp.UnivariateSpline(
                np.log10(ab_data[metric_a]), 
                np.log10(ab_data[metric_b]),
                s=len(ab_data)  # Increased from 0.1 to 1.0 times the data length
            )
            
            # Generate points for smooth curve
            x_smooth = np.logspace(
                np.log10(ab_data[metric_a].min()),
                np.log10(ab_data[metric_a].max()),
                100
            )
            y_smooth = 10 ** spl(np.log10(x_smooth))
            
            # Create figure
            fig_ab = go.Figure()
            
            # Add scatter plot (using original data)
            fig_ab.add_trace(go.Scatter(
                x=all_profiles_data[metric_a],
                y=all_profiles_data[metric_b],
                mode='markers',
                name='Data',
                marker=dict(size=4)
            ))
            
            # Add spline
            fig_ab.add_trace(go.Scatter(
                x=x_smooth,
                y=y_smooth,
                mode='lines',
                name='Trend',
                line=dict(color='red', width=2)
            ))
            
            fig_ab.update_layout(
                title={
                    'text': f"{metric_a} vs {metric_b}",
                    'x': 0.5,
                    'xanchor': 'center'
                },
                xaxis_title=metric_a,
                yaxis_title=metric_b,
                xaxis_type="log",
                yaxis_type="log",
                showlegend=False,
                height=400,
                margin=dict(t=30, l=10, r=10, b=10)
            )
            st.plotly_chart(fig_ab, use_container_width=True)
        
        # B vs A plot
        with corr_col2:
            # Prepare data for spline fitting
            ba_data = all_profiles_data[[metric_b, metric_a]].copy()
            ba_data = ba_data[ba_data[[metric_b, metric_a]].gt(0).all(axis=1)]  # Remove zeros and negatives
            ba_data = ba_data.sort_values(metric_b)
            ba_data = ba_data.groupby(metric_b)[metric_a].mean().reset_index()  # Handle duplicates
            
            # Fit spline with higher smoothing factor
            spl = interp.UnivariateSpline(
                np.log10(ba_data[metric_b]), 
                np.log10(ba_data[metric_a]),
                s=len(ba_data)  # Increased from 0.1 to 1.0 times the data length
            )
            
            # Generate points for smooth curve
            x_smooth = np.logspace(
                np.log10(ba_data[metric_b].min()),
                np.log10(ba_data[metric_b].max()),
                100
            )
            y_smooth = 10 ** spl(np.log10(x_smooth))
            
            # Create figure
            fig_ba = go.Figure()
            
            # Add scatter plot (using original data)
            fig_ba.add_trace(go.Scatter(
                x=all_profiles_data[metric_b],
                y=all_profiles_data[metric_a],
                mode='markers',
                name='Data',
                marker=dict(size=4)
            ))
            
            # Add spline
            fig_ba.add_trace(go.Scatter(
                x=x_smooth,
                y=y_smooth,
                mode='lines',
                name='Trend',
                line=dict(color='red', width=2)
            ))
            
            fig_ba.update_layout(
                title={
                    'text': f"{metric_b} vs {metric_a}",
                    'x': 0.5,
                    'xanchor': 'center'
                },
                xaxis_title=metric_b,
                yaxis_title=metric_a,
                xaxis_type="log",
                yaxis_type="log",
                showlegend=False,
                height=400,
                margin=dict(t=30, l=10, r=10, b=10)
            )
            st.plotly_chart(fig_ba, use_container_width=True)
    else:
        with corr_col1:
            st.warning("Please select different metrics for correlation analysis")

# Audio analysis section
st.write('# Audio Analysis')

# Language comparison section
with st.expander("What are the metrics of videos in each language?", expanded=False):
    lang_col1, lang_col2 = st.columns([3, 1])
    
    # Metric selection column
    with lang_col2:
        st.write("Select Metric")
        lang_metric = st.selectbox(
            "Metric",
            options=["Views", "Likes", "Comments", "Reposts", "Duration"],
            key="lang_metric"
        )
    
    # Plot column
    with lang_col1:
        # Get unique languages and create KDE plot
        languages = all_profiles_data['Language'].dropna().unique()
        
        # Create figure
        fig = go.Figure()
        
        # Add a KDE for each language
        for lang in languages:
            lang_data = all_profiles_data[all_profiles_data['Language'] == lang][lang_metric]
            
            # Only plot if we have enough data points (at least 3)
            if len(lang_data) > 3 and not lang_data.empty:
                # Remove zeros and negative values for log scale
                lang_data = lang_data[lang_data > 0]
                
                if len(lang_data) > 3:  # Check again after filtering
                    # Calculate KDE
                    kde_points = np.logspace(
                        np.log10(lang_data.min()),
                        np.log10(lang_data.max()),
                        100
                    )
                    kde = scipy.stats.gaussian_kde(np.log10(lang_data))
                    kde_values = kde(np.log10(kde_points))
                    
                    # Add trace
                    fig.add_trace(go.Scatter(
                        x=kde_points,
                        y=kde_values,
                        name=lang,
                        mode='lines',
                        fill='tonexty',
                        line=dict(width=2)
                    ))
        
        # Update layout
        fig.update_layout(
            title={
                'text': f"{lang_metric} Distribution by Language",
                'x': 0.5,
                'xanchor': 'center'
            },
            xaxis_title=lang_metric,
            yaxis_title="Density",
            xaxis_type="log",
            height=400,
            showlegend=True
        )
        
        st.plotly_chart(fig, use_container_width=True)

    st.write("---")  # Add separator between plots
    
    # Time series by language
    ts_lang_col1, ts_lang_col2 = st.columns([3, 1])
    
    # Define language toggles
    with ts_lang_col2:
        st.caption("Toggle languages to show/hide in the plot")  # Use caption instead of write with help
        
        # Split languages into two equal groups
        mid_point = len(languages) // 2
        left_languages = languages[:mid_point]
        right_languages = languages[mid_point:]
        
        # Create two columns for checkboxes
        toggle_col1, toggle_col2 = st.columns(2)
        
        # Left column of toggles
        with toggle_col1:
            left_toggles = {
                lang.title(): st.checkbox(
                    "  " + lang.title(),
                    value=True,
                    key=f"lang_toggle_left_{lang}"
                )
                for lang in left_languages
            }
        
        # Right column of toggles
        with toggle_col2:
            right_toggles = {
                lang.title(): st.checkbox(
                    "  " + lang.title(),
                    value=True,
                    key=f"lang_toggle_right_{lang}"
                )
                for lang in right_languages
            }
        
        # Combine toggles
        language_toggles = {**left_toggles, **right_toggles}
    
    # Plot column
    with ts_lang_col1:
        # Create figure
        fig = go.Figure()
        
        # Add a trace for each selected language
        for lang, is_selected in language_toggles.items():
            if is_selected:
                # Filter data for this language
                lang_data = all_profiles_data[all_profiles_data['Language'].str.title() == lang]
                
                if not lang_data.empty:
                    # Calculate daily averages for this language
                    daily_avg = lang_data.groupby('Upload Date')[lang_metric].mean()
                    
                    # Add trace
                    fig.add_trace(
                        go.Scatter(
                            x=daily_avg.index,
                            y=daily_avg.values,
                            name=lang,
                            mode='markers',
                            marker=dict(size=4)
                        )
                    )
        
        # Update layout
        fig.update_layout(
            title={
                'text': f"{lang_metric} Over Time by Language",
                'x': 0.5,
                'xanchor': 'center'
            },
            xaxis_title="Date",
            yaxis_title=lang_metric,
            height=400,
            showlegend=True,
            yaxis_type="log"
        )
        
        st.plotly_chart(fig, use_container_width=True)

# Category distribution by language section
with st.expander("What content is made in each language?", expanded=False):
    # Get valid languages (remove NA/None and 'unknown')
    languages = all_profiles_data['Language'].dropna()
    languages = languages[languages.str.lower() != 'unknown']
    languages = languages.unique()
    
    # Calculate category proportions for each language
    category_props = []
    
    for lang in languages:
        lang_data = all_profiles_data[all_profiles_data['Language'] == lang]
        if not lang_data.empty:
            # Calculate proportions
            props = lang_data['Category'].value_counts(normalize=True).reset_index()
            props.columns = ['Category', 'Proportion']
            props['Language'] = lang.title()  # Capitalize language names
            category_props.append(props)
    
    if category_props:  # Only proceed if we have data
        # Combine all proportions
        category_props = pd.concat(category_props, ignore_index=True)
        
        # Create subplot grid
        n_langs = len(languages)
        n_cols = 3  # Number of columns in the grid
        n_rows = (n_langs + n_cols - 1) // n_cols  # Ceiling division for number of rows
        
        # Create subplot grid
        fig = make_subplots(
            rows=n_rows, 
            cols=n_cols,
            subplot_titles=[lang.title() for lang in languages],
            vertical_spacing=0.12,  # Reduced vertical spacing
            horizontal_spacing=0.08  # Reduced horizontal spacing
        )
        
        # Add bars for each language
        for i, lang in enumerate(languages):
            row = (i // n_cols) + 1
            col = (i % n_cols) + 1
            
            lang_props = category_props[category_props['Language'] == lang.title()]
            
            fig.add_trace(
                go.Bar(
                    x=lang_props['Category'],
                    y=lang_props['Proportion'],
                    name=lang.title(),
                    showlegend=False,
                    width=0.7  # Make bars slightly thinner
                ),
                row=row,
                col=col
            )
            
            # Update layout for this subplot
            fig.update_xaxes(
                tickangle=45,
                row=row,
                col=col,
                tickfont=dict(size=7),  # Even smaller font
                tickmode='array',
                ticktext=lang_props['Category'],
                tickvals=lang_props['Category']
            )
            fig.update_yaxes(
                range=[0, 1],
                tickformat='.0%',
                row=row,
                col=col,
                tickfont=dict(size=7),  # Even smaller font
                title=None,
                nticks=5  # Reduce number of y-axis ticks
            )
        
        # Update overall layout
        fig.update_layout(
            height=200 * n_rows,  # Reduced height per row
            title={
                'text': "Category Distribution by Language",
                'y': 0.99,  # Move title to very top
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'
            },
            showlegend=False,
            margin=dict(t=50, l=5, r=5, b=20)  # Increased top margin from 30 to 50
        )
        
        st.plotly_chart(fig, use_container_width=True)

# Category metrics section
with st.expander("What are the metrics of videos in each category?", expanded=False):
    cat_col1, cat_col2 = st.columns([3, 1])
    
    # Metric selection column
    with cat_col2:
        st.write("Select Metric")
        cat_metric = st.selectbox(
            "Metric",
            options=["Views", "Likes", "Comments", "Reposts", "Duration"],
            key="cat_metric"
        )
    
    # Plot column
    with cat_col1:
        # Get unique categories and create KDE plot
        categories = all_profiles_data['Category'].dropna().unique()
        
        # Create figure
        fig = go.Figure()
        
        # Add a KDE for each category
        for category in categories:
            cat_data = all_profiles_data[all_profiles_data['Category'] == category][cat_metric]
            
            # Only plot if we have enough data points (at least 3)
            if len(cat_data) > 3 and not cat_data.empty:
                # Remove zeros and negative values for log scale
                cat_data = cat_data[cat_data > 0]
                
                if len(cat_data) > 3:  # Check again after filtering
                    # Calculate KDE
                    kde_points = np.logspace(
                        np.log10(cat_data.min()),
                        np.log10(cat_data.max()),
                        100
                    )
                    kde = scipy.stats.gaussian_kde(np.log10(cat_data))
                    kde_values = kde(np.log10(kde_points))
                    
                    # Add trace
                    fig.add_trace(go.Scatter(
                        x=kde_points,
                        y=kde_values,
                        name=category,
                        mode='lines',
                        fill='tonexty',
                        line=dict(width=2)
                    ))
        
        # Update layout
        fig.update_layout(
            title={
                'text': f"{cat_metric} Distribution by Category",
                'x': 0.5,
                'xanchor': 'center'
            },
            xaxis_title=cat_metric,
            yaxis_title="Density",
            xaxis_type="log",
            height=400,
            showlegend=True
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Time series by category
    st.write("---")  # Add separator between plots
    
    # Create figure
    fig = go.Figure()
    
    # Add a trace for each category
    for category in categories:
        # Filter data for this category
        cat_data = all_profiles_data[all_profiles_data['Category'] == category]
        
        if not cat_data.empty:
            # Calculate daily averages for this category
            daily_avg = cat_data.groupby('Upload Date')[cat_metric].mean()
            
            # Add trace
            fig.add_trace(
                go.Scatter(
                    x=daily_avg.index,
                    y=daily_avg.values,
                    name=category,
                    mode='markers',
                    marker=dict(size=4)
                )
            )
    
    # Update layout
    fig.update_layout(
        title={
            'text': f"{cat_metric} Over Time by Category",
            'x': 0.5,
            'xanchor': 'center'
        },
        xaxis_title="Date",
        yaxis_title=cat_metric,
        height=400,
        showlegend=True,
        yaxis_type="log"
    )
    
    st.plotly_chart(fig, use_container_width=True)

# Keyword analysis section
with st.expander("What are the most popular keywords in English for each category of content?", expanded=False):
    # Filter for English content
    english_content = all_profiles_data[all_profiles_data['Language'] == 'english']
    
    # Process keywords and categories
    keyword_data = []
    
    for _, row in english_content.iterrows():
        category = row['Category']
        keywords = row['Keywords'].tolist()
        
        # Add each keyword with its category
        for keyword in keywords:
            keyword_data.append({
                'Category': category,
                'Keyword': keyword
            })
    
    # Convert to DataFrame and calculate proportions
    keyword_df = pd.DataFrame(keyword_data)
    
    # Calculate proportions within each category
    category_counts = keyword_df.groupby('Category')['Keyword'].count()
    keyword_props = (keyword_df.groupby(['Category', 'Keyword'])
                    .size()
                    .reset_index(name='count'))
    
    # Calculate proportion within each category
    keyword_props['proportion'] = keyword_props.apply(
        lambda x: x['count'] / category_counts[x['Category']], 
        axis=1
    )
    
    # Get top 20 categories by video count
    top_categories = category_counts.nlargest(20).index
    
    # Create subplot grid (4 rows x 5 columns)
    fig = make_subplots(
        rows=4, 
        cols=5,
        subplot_titles=[cat for cat in top_categories],
        vertical_spacing=0.15,
        horizontal_spacing=0.05
    )
    
    # Add bar plots for each category
    for i, category in enumerate(top_categories):
        row = (i // 5) + 1  # Integer division by 5 for row number (1-4)
        col = (i % 5) + 1   # Modulo 5 for column number (1-5)
        
        # Get top 10 keywords for this category
        cat_data = (keyword_props[keyword_props['Category'] == category]
                   .nlargest(10, 'proportion'))
        
        # Add bars
        fig.add_trace(
            go.Bar(
                x=cat_data['Keyword'],
                y=cat_data['proportion'],
                text=cat_data['proportion'].apply(lambda x: f'{x:.1%}'),
                textposition='auto',
                showlegend=False
            ),
            row=row,
            col=col
        )
        
        # Update axes
        fig.update_xaxes(
            tickangle=45,
            row=row,
            col=col,
            tickfont=dict(size=8),
            title=None
        )
        fig.update_yaxes(
            range=[0, 1],
            tickformat='.0%',
            row=row,
            col=col,
            tickfont=dict(size=8),
            title=None,
            nticks=5
        )
    
    # Update overall layout
    fig.update_layout(
        height=800,  # Taller to accommodate all plots
        title={
            'text': "Top Keywords by Category in English Content",
            'x': 0.5,
            'xanchor': 'center'
        },
        showlegend=False,
        margin=dict(t=50, l=20, r=20, b=50)
    )
    
    st.plotly_chart(fig, use_container_width=True)

# Interaction rates section
with st.expander("What content categories have the most interaction per view?", expanded=False):
    # Calculate average interaction rates for each category
    interaction_data = []
    
    for category in all_profiles_data['Category'].unique():
        cat_data = all_profiles_data[all_profiles_data['Category'] == category]
        
        # Calculate mean rates
        likes_rate = (cat_data['Likes'] / cat_data['Views']).mean()
        comments_rate = (cat_data['Comments'] / cat_data['Views']).mean()
        reposts_rate = (cat_data['Reposts'] / cat_data['Views']).mean()
        
        interaction_data.append({
            'Category': category,
            'Likes Rate': likes_rate,
            'Comments Rate': comments_rate,
            'Reposts Rate': reposts_rate
        })
    
    interaction_df = pd.DataFrame(interaction_data)
    
    # Create plots for each metric
    metrics = ['Likes Rate', 'Comments Rate', 'Reposts Rate']
    
    for metric in metrics:
        # Sort data by rate and remove zeros
        sorted_data = interaction_df[interaction_df[metric] > 0].sort_values(metric, ascending=False)
        
        if not sorted_data.empty:
            # Create bar plot
            fig = go.Figure()
            
            fig.add_trace(
                go.Bar(
                    x=sorted_data['Category'],
                    y=sorted_data[metric],
                    text=sorted_data[metric].apply(lambda x: f'{x:.1%}'),
                    textposition='auto'
                )
            )
            
            # Update layout
            fig.update_layout(
                title={
                    'text': f"Average {metric.replace('Rate', '')}per View by Category",
                    'x': 0.5,
                    'xanchor': 'center'
                },
                xaxis_title=None,
                yaxis_title="Interaction Rate",
                height=400,
                showlegend=False,
                yaxis=dict(tickformat='.1%'),
                xaxis=dict(tickangle=45),
                margin=dict(b=100)
            )
            
            st.plotly_chart(fig, use_container_width=True)

# Interaction rate consistency section
with st.expander("What content categories are the most consistent in interactions per view?", expanded=False):
    # Calculate standard deviation of interaction rates for each category
    consistency_data = []
    
    for category in all_profiles_data['Category'].unique():
        cat_data = all_profiles_data[all_profiles_data['Category'] == category]
        
        # Only calculate if we have enough data points and non-zero views
        if len(cat_data) > 1 and (cat_data['Views'] > 0).any():
            # Calculate std of rates
            likes_std = (cat_data['Likes'] / cat_data['Views']).std()
            comments_std = (cat_data['Comments'] / cat_data['Views']).std()
            reposts_std = (cat_data['Reposts'] / cat_data['Views']).std()
            
            # Only add if all values are valid
            if pd.notna([likes_std, comments_std, reposts_std]).all():
                consistency_data.append({
                    'Category': category,
                    'Likes Rate Std': likes_std,
                    'Comments Rate Std': comments_std,
                    'Reposts Rate Std': reposts_std
                })
    
    consistency_df = pd.DataFrame(consistency_data)
    
    # Create plots for each metric
    metrics = ['Likes Rate Std', 'Comments Rate Std', 'Reposts Rate Std']
    
    for metric in metrics:
        # Sort data by standard deviation (ascending for consistency - lower is more consistent)
        sorted_data = consistency_df.sort_values(metric, ascending=True)
        
        # Create bar plot
        fig = go.Figure()
        
        fig.add_trace(
            go.Bar(
                x=sorted_data['Category'],
                y=sorted_data[metric],
                text=sorted_data[metric].apply(lambda x: f'{x:.1%}'),
                textposition='auto'
            )
        )
        
        # Update layout
        fig.update_layout(
            title={
                'text': f"Consistency in {metric.replace('Rate Std', '')}per View by Category",
                'x': 0.5,
                'xanchor': 'center'
            },
            xaxis_title=None,
            yaxis_title="Standard Deviation of Interaction Rate",
            height=400,
            showlegend=False,
            yaxis=dict(tickformat='.1%'),
            xaxis=dict(tickangle=45),
            margin=dict(b=100)
        )
        
        st.plotly_chart(fig, use_container_width=True)

# Predictive keywords section
with st.expander("Controlling for profile and duration, what keywords are predictive of view count in each category of content?", expanded=False):
    # Create columns for plot and selector
    pred_col1, pred_col2 = st.columns([3, 1])
    
    # Category selection column
    with pred_col2:
        st.write("Select Category")
        selected_category = st.selectbox(
            "Category",
            options=all_profiles_data['Category'].unique(),
            key="pred_category"
        )
        
        # Check if category has enough samples
        category_size = len(all_profiles_data[all_profiles_data['Category'] == selected_category])
        if category_size < 100:
            st.warning(f"Not enough samples for {selected_category} (needs 100, has {category_size})")
        else:
            # Get predictive keywords
            pred_keywords = top_predictive_keywords_for_category(selected_category)
            
            # Plot column
            with pred_col1:
                # Create bar plot
                fig = go.Figure()
                
                # Add bars with different colors for positive/negative values
                fig.add_trace(
                    go.Bar(
                        x=pred_keywords['Keyword'],
                        y=pred_keywords['Impact'],
                        text=pred_keywords['Impact'].apply(lambda x: f'{x:,.0f}'),
                        textposition='outside',
                        marker_color=pred_keywords['Impact'].apply(
                            lambda x: 'rgb(55, 126, 184)' if x >= 0 else 'rgb(228, 26, 28)'
                        )
                    )
                )
                
                # Update layout
                fig.update_layout(
                    title={
                        'text': f"Keywords Most Predictive of Views in {selected_category}",
                        'x': 0.5,
                        'xanchor': 'center'
                    },
                    xaxis_title=None,
                    yaxis_title="Impact on Views",
                    height=500,  # Increased height
                    showlegend=False,
                    xaxis=dict(tickangle=45),
                    margin=dict(l=150, r=150, t=50, b=100),  # Increased left/right margins
                    yaxis=dict(
                        zeroline=True,
                        zerolinewidth=2,
                        zerolinecolor='black',
                        automargin=True  # Added automargin
                    ),
                    bargap=0.2  # Added gap between bars
                )
                
                st.plotly_chart(fig, use_container_width=True)


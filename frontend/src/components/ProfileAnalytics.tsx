import React, { useState, useEffect } from 'react';
import { Box, Typography, CircularProgress, Grid, Card, CardContent } from '@mui/material';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';

interface Video {
  id: string
  title: string
  description: string
  upload_date: Date
  like_count: number
  repost_count: number
  comment_count: number
  view_count: number
  duration: number
  transcript: string
  category: string
  summary: string
  keywords: string[]
  language: string
  profile_name: string
}

interface ProfileStats {
  stats: {
    view_count: MetricStats;
    like_count: MetricStats;
    repost_count: MetricStats;
    comment_count: MetricStats;
  };
  video_count: number;
  sample_videos: VideoData[];
}

interface MetricStats {
  total: number;
  min: number;
  max: number;
  mean: number;
  median: number;
  percentile_25: number;
  percentile_75: number;
}

interface VideoData {
  id: string;
  title: string;
  description: string;
  upload_date: string;
  duration: number;
  view_count: number;
  like_count: number;
  repost_count: number;
  comment_count: number;
  transcript: string;
  processed_at: string;
  category: string;
  summary: string;
  language: string;
}

interface ProfileAnalyticsProps {
  profileName: string;
}

export const ProfileAnalytics: React.FC<ProfileAnalyticsProps> = ({ profileName }) => {
  const [data, setData] = useState<ProfileStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        const response = await fetch(`/api/profile?profile=${encodeURIComponent(profileName)}`);
        if (!response.ok) {
          throw new Error('Failed to fetch profile data');
        }
        const result = await response.json();
        setData(result);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [profileName]);

  if (loading) {
    return <CircularProgress />;
  }

  if (error || !data) {
    return <Typography color="error">{error || 'No data available'}</Typography>;
  }

  const formatNumber = (num: number) => {
    if (num >= 1000000) {
      return `${(num / 1000000).toFixed(1)}M`;
    }
    if (num >= 1000) {
      return `${(num / 1000).toFixed(1)}K`;
    }
    return num.toString();
  };

  const metricsData = [
    {
      name: 'Views',
      total: data.stats.view_count.total,
      avg: data.stats.view_count.mean,
      median: data.stats.view_count.median,
    },
    {
      name: 'Likes',
      total: data.stats.like_count.total,
      avg: data.stats.like_count.mean,
      median: data.stats.like_count.median,
    },
    {
      name: 'Reposts',
      total: data.stats.repost_count.total,
      avg: data.stats.repost_count.mean,
      median: data.stats.repost_count.median,
    },
    {
      name: 'Comments',
      total: data.stats.comment_count.total,
      avg: data.stats.comment_count.mean,
      median: data.stats.comment_count.median,
    },
  ];

  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" gutterBottom>
        Profile Analytics: {profileName}
      </Typography>
      
      <Typography variant="h6" gutterBottom>
        Total Videos: {data.video_count}
      </Typography>

      <Box sx={{ mt: 4 }}>
        <BarChart width={800} height={400} data={metricsData}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="name" />
          <YAxis tickFormatter={formatNumber} />
          <Tooltip formatter={formatNumber} />
          <Legend />
          <Bar dataKey="total" fill="#8884d8" name="Total" />
          <Bar dataKey="avg" fill="#82ca9d" name="Average" />
          <Bar dataKey="median" fill="#ffc658" name="Median" />
        </BarChart>
      </Box>

      <Grid container spacing={2} sx={{ mt: 4 }}>
        {data.sample_videos.slice(0, 6).map((video) => (
          <Grid item xs={12} sm={6} md={4} key={video.id}>
            <Card>
              <CardContent>
                <Typography variant="h6" noWrap title={video.title}>
                  {video.title}
                </Typography>
                <Typography color="textSecondary" gutterBottom>
                  {new Date(video.upload_date).toLocaleDateString()}
                </Typography>
                <Typography variant="body2" noWrap title={video.summary}>
                  {video.summary}
                </Typography>
                <Box sx={{ mt: 1 }}>
                  <Typography variant="body2">
                    Views: {formatNumber(video.view_count)}
                  </Typography>
                  <Typography variant="body2">
                    Likes: {formatNumber(video.like_count)}
                  </Typography>
                </Box>
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>
    </Box>
  );
}; 
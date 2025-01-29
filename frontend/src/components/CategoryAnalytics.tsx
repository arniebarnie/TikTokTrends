import { useEffect, useRef, useState } from 'react'
import * as d3 from 'd3'
import cloud from 'd3-cloud'

interface CategoryMetrics {
  date: Date
  video_count: number
  like_count: number
  comment_count: number
  repost_count: number
  view_count: number
  duration: number
}

interface TopVideo {
  id: string
  profile_name: string
  language: string
  views: number
  likes: number
  comments: number
  reposts: number
  summary: string
}

interface KeywordStats {
  keyword: string
  count: number
  proportion: number
}

interface CategoryData {
  [key: string]: {
    dailyMetrics: CategoryMetrics[]
    topVideos: TopVideo[]
    keywords: KeywordStats[]
  }
}

interface CloudWord {
  text: string | undefined
  size: number
  x?: number
  y?: number
  rotate?: number
}

// Mock data
const mockCategoryData: CategoryData = {
  "Entertainment": {
    dailyMetrics: [
      {
        date: new Date("2024-01-01"),
        video_count: 100,
        like_count: 50000,
        comment_count: 2000,
        repost_count: 1000,
        view_count: 100000,
        duration: 3000
      },
      {
        date: new Date("2024-01-02"),
        video_count: 120,
        like_count: 55000,
        comment_count: 2200,
        repost_count: 1100,
        view_count: 110000,
        duration: 3300
      },
      {
        date: new Date("2024-01-03"),
        video_count: 110,
        like_count: 52000,
        comment_count: 2100,
        repost_count: 1050,
        view_count: 105000,
        duration: 3150
      }
    ],
    topVideos: [
      {
        id: "vid1",
        profile_name: "creator1",
        language: "English",
        views: 50000,
        likes: 25000,
        comments: 1000,
        reposts: 500,
        summary: "This is a viral video about entertainment"
      },
      {
        id: "vid2",
        profile_name: "creator2",
        language: "English",
        views: 45000,
        likes: 22000,
        comments: 900,
        reposts: 450,
        summary: "Another popular entertainment video"
      }
    ],
    keywords: [
      { keyword: "funny", count: 500, proportion: 0.8 },
      { keyword: "music", count: 400, proportion: 0.6 },
      { keyword: "dance", count: 300, proportion: 0.5 },
      { keyword: "comedy", count: 250, proportion: 0.4 },
      { keyword: "viral", count: 200, proportion: 0.3 },
      { keyword: "trending", count: 150, proportion: 0.25 },
      { keyword: "challenge", count: 100, proportion: 0.2 },
      { keyword: "fun", count: 80, proportion: 0.15 },
      { keyword: "entertainment", count: 60, proportion: 0.1 },
      { keyword: "happy", count: 40, proportion: 0.05 }
    ]
  },
  "Education": {
    dailyMetrics: [
      {
        date: new Date("2024-01-01"),
        video_count: 80,
        like_count: 40000,
        comment_count: 1500,
        repost_count: 800,
        view_count: 90000,
        duration: 2500
      }
    ],
    topVideos: [
      {
        id: "vid3",
        profile_name: "creator3",
        language: "English",
        views: 40000,
        likes: 20000,
        comments: 800,
        reposts: 400,
        summary: "Educational content about science"
      }
    ],
    keywords: [
      { keyword: "education", count: 400, proportion: 0.7 },
      { keyword: "learning", count: 300, proportion: 0.5 }
    ]
  }
} 

const METRICS = [
  { value: 'video_count', label: 'Videos' },
  { value: 'like_count', label: 'Likes' },
  { value: 'comment_count', label: 'Comments' },
  { value: 'repost_count', label: 'Reposts' },
  { value: 'view_count', label: 'Views' },
  { value: 'duration', label: 'Duration' }
]

export default function CategoryAnalytics() {
  const [selectedCategory, setSelectedCategory] = useState<string>("")
  const [selectedMetrics, setSelectedMetrics] = useState<string[]>([])
  const timeSeriesRef = useRef<HTMLDivElement>(null)
  const wordCloudRef = useRef<HTMLDivElement>(null)
  
  const categories = Object.keys(mockCategoryData)

  useEffect(() => {
    if (selectedCategory) {
      createVisualizations()
    }
  }, [selectedCategory, selectedMetrics])

  const getCategorySummary = (data: typeof mockCategoryData[keyof typeof mockCategoryData]) => {
    if (!data || !data.dailyMetrics.length) return {
      totalVideos: 0,
      totalLikes: 0,
      totalComments: 0,
      totalReposts: 0,
      totalViews: 0
    }

    const latest = data.dailyMetrics[data.dailyMetrics.length - 1]
    return {
      totalVideos: latest.video_count,
      totalLikes: latest.like_count,
      totalComments: latest.comment_count,
      totalReposts: latest.repost_count,
      totalViews: latest.view_count
    }
  }

  const createVisualizations = () => {
    if (!selectedCategory) return
    
    const data = mockCategoryData[selectedCategory]
    createTimeSeriesCharts(data.dailyMetrics)
    createWordCloud(data.keywords)
  }

  const createTimeSeriesCharts = (data: CategoryMetrics[]) => {
    if (!timeSeriesRef.current) return
    d3.select(timeSeriesRef.current).selectAll("*").remove()

    const containerWidth = timeSeriesRef.current.clientWidth
    const chartWidth = (containerWidth - (selectedMetrics.length - 1) * 20) / selectedMetrics.length
    const height = 300
    const margin = { top: 20, right: 30, bottom: 30, left: 60 }

    selectedMetrics.forEach((metric, i) => {
      const svg = d3.select(timeSeriesRef.current)
        .append("svg")
        .attr("width", chartWidth)
        .attr("height", height)
        .style("margin-right", i < selectedMetrics.length - 1 ? "20px" : "0")
        .append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`)

      const x = d3.scaleTime()
        .domain(d3.extent(data, d => d.date) as [Date, Date])
        .range([0, chartWidth - margin.left - margin.right])

      const y = d3.scaleLinear()
        .domain([0, d3.max(data, d => d[metric as keyof CategoryMetrics] as number) as number])
        .range([height - margin.top - margin.bottom, 0])

      // Add X axis
      svg.append("g")
        .attr("transform", `translate(0,${height - margin.top - margin.bottom})`)
        .call(d3.axisBottom(x)
          .ticks(d3.timeMonth.every(1))
          .tickFormat((d: any) => d3.timeFormat("%b %Y")(d)))
        .style("color", "black")

      // Add Y axis
      svg.append("g")
        .call(d3.axisLeft(y))
        .style("color", "black")

      // Add line
      const line = d3.line<CategoryMetrics>()
        .x(d => x(d.date))
        .y(d => y(d[metric as keyof CategoryMetrics] as number))

      svg.append("path")
        .datum(data)
        .attr("fill", "none")
        .attr("stroke", "#2563eb")
        .attr("stroke-width", 2)
        .attr("d", line)

      // Add title
      svg.append("text")
        .attr("x", 10)
        .attr("y", 10)
        .text(METRICS.find(m => m.value === metric)?.label || metric)
        .style("font-size", "12px")
        .style("fill", "black")
    })
  }

  const createWordCloud = (keywords: KeywordStats[]) => {
    if (!wordCloudRef.current) return
    d3.select(wordCloudRef.current).selectAll("*").remove()

    const width = wordCloudRef.current.clientWidth
    const height = 300

    const layout = cloud()
      .size([width, height])
      .words(keywords.map(d => ({
        text: d.keyword,
        size: 10 + (d.proportion * 50)
      } as CloudWord)))
      .padding(5)
      .rotate(0)
      .fontSize((d: any) => d.size || 0)
      .on("end", draw)

    function draw(words: CloudWord[]) {
      d3.select(wordCloudRef.current)
        .append("svg")
        .attr("width", width)
        .attr("height", height)
        .append("g")
        .attr("transform", `translate(${width/2},${height/2})`)
        .selectAll("text")
        .data(words)
        .enter()
        .append("text")
        .style("font-size", d => `${d.size}px`)
        .style("fill", "#2563eb")
        .attr("text-anchor", "middle")
        .attr("transform", d => `translate(${d.x},${d.y})`)
        .text(d => d.text || '')
    }

    layout.start()
  }

  return (
    <div className="bg-white p-6 rounded-lg shadow-lg text-black">
      <div className="mb-6 flex flex-wrap gap-4">
        <select 
          value={selectedCategory}
          onChange={(e) => setSelectedCategory(e.target.value)}
          className="p-2 border rounded"
        >
          <option value="">Select Category</option>
          {categories.map(category => (
            <option key={category} value={category}>{category}</option>
          ))}
        </select>

        <div className="flex gap-2 flex-wrap">
          {METRICS.map(metric => (
            <label key={metric.value} className="flex items-center gap-1">
              <input
                type="checkbox"
                value={metric.value}
                checked={selectedMetrics.includes(metric.value)}
                onChange={(e) => {
                  if (e.target.checked) {
                    setSelectedMetrics([...selectedMetrics, metric.value])
                  } else {
                    setSelectedMetrics(selectedMetrics.filter(m => m !== metric.value))
                  }
                }}
              />
              {metric.label}
            </label>
          ))}
        </div>
      </div>

      {selectedCategory && (
        <>
          <div className="mb-6 grid grid-cols-5 gap-4">
            {Object.entries(getCategorySummary(mockCategoryData[selectedCategory]))
              .map(([key, value]) => (
                <div key={key} className="p-3 border rounded bg-gray-50">
                  <div className="text-sm text-gray-600 mb-1">
                    {key.replace(/([A-Z])/g, ' $1').replace(/^./, str => str.toUpperCase())}
                  </div>
                  <div className="font-semibold">
                    {value.toLocaleString()}
                  </div>
                </div>
              ))}
          </div>

          {selectedMetrics.length > 0 && (
            <div ref={timeSeriesRef} className="mb-6 flex" />
          )}

          <div className="mb-6 overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="bg-gray-50">
                  <th className="px-4 py-2">Video ID</th>
                  <th className="px-4 py-2">Profile</th>
                  <th className="px-4 py-2">Language</th>
                  <th className="px-4 py-2">Views</th>
                  <th className="px-4 py-2">Likes</th>
                  <th className="px-4 py-2">Comments</th>
                  <th className="px-4 py-2">Reposts</th>
                  <th className="px-4 py-2">Summary</th>
                </tr>
              </thead>
              <tbody>
                {mockCategoryData[selectedCategory].topVideos.map(video => (
                  <tr key={video.id} className="border-t">
                    <td className="px-4 py-2">{video.id}</td>
                    <td className="px-4 py-2">{video.profile_name}</td>
                    <td className="px-4 py-2">{video.language}</td>
                    <td className="px-4 py-2">{video.views.toLocaleString()}</td>
                    <td className="px-4 py-2">{video.likes.toLocaleString()}</td>
                    <td className="px-4 py-2">{video.comments.toLocaleString()}</td>
                    <td className="px-4 py-2">{video.reposts.toLocaleString()}</td>
                    <td className="px-4 py-2">{video.summary}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="flex gap-4">
            <div ref={wordCloudRef} className="w-1/2 h-[300px]" />
            
            <div className="w-1/2">
              <table className="min-w-full">
                <thead>
                  <tr className="bg-gray-50">
                    <th className="px-4 py-2 text-left">Keyword</th>
                    <th className="px-4 py-2 text-right">Frequency</th>
                    <th className="px-4 py-2 text-right">Proportion</th>
                  </tr>
                </thead>
                <tbody>
                  {mockCategoryData[selectedCategory].keywords
                    .sort((a, b) => b.count - a.count)
                    .map(keyword => (
                      <tr key={keyword.keyword} className="border-t">
                        <td className="px-4 py-2">{keyword.keyword}</td>
                        <td className="px-4 py-2 text-right">{keyword.count.toLocaleString()}</td>
                        <td className="px-4 py-2 text-right">{(keyword.proportion * 100).toFixed(1)}%</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  )
} 
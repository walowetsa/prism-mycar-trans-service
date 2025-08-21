"use client";

import { useState, useEffect, useMemo, useRef } from "react";
import Link from "next/link";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
  Legend,
  Area,
  AreaChart,
} from "recharts";
import { BiSolidHome } from "react-icons/bi";

// Comprehensive interface to capture all possible fields from Supabase
interface CallOverviewData {
  // Core identification fields
  id?: string | number;
  contact_id: string;
  agent_username: string;
  initiation_timestamp: string;

  // Call routing and categorization
  queue_name?: string;
  disposition_title?: string;
  campaign_name?: string;
  customer_cli?: string;

  // Call metrics
  call_duration: {
    minutes: number;
    seconds: number;
  };

  // AI analysis fields
  call_summary?: string;
  primary_category?: string;
  categories?: string;
  sentiment_analysis?: string;
  transcript_text?: string;

  // Additional analysis fields that might exist
  keywords?: string;
  topics?: string;
  satisfaction_score?: number;
  resolution_status?: string;
  callback_requested?: boolean;
  language?: string;
  channel?: string;

  // System fields
  created_at: string;
  updated_at: string;
  processed_at?: string;

  // Catch-all for any additional fields
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [key: string]: any;
}

interface FilterState {
  search: string;
  agent: string;
  queue: string;
  disposition: string;
  campaign: string;
  dateRange: string;
  specificDate: string; // Added for specific date filtering
  dateFilterType: 'range' | 'specific'; // Added to toggle between range and specific date
}

interface SentimentCounts {
  positive: number;
  neutral: number;
  negative: number;
}

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
}

const SENTIMENT_COLORS = {
  positive: "#10B981",
  neutral: "#6B7280",
  negative: "#EF4444",
};

// Sample queries to help guide users
const SAMPLE_QUERIES = [
  "Give me a breakdown of all New Business Leads.",
  "What are the most common reasons for declining offers?",
  "What agents have the highest conversion rates?",
  "Can you give me an end of summary summary?",
  "Give me a breakdown of call sentiments.",
  "Any suggestions for improving the number of leads generated?",
];

// Custom Markdown Message Component with enhanced styling
const MarkdownMessage = ({
  content,
  isUser,
}: {
  content: string;
  isUser: boolean;
}) => {
  return (
    <div
      className={`markdown-content ${isUser ? "user-message" : "ai-message"}`}
    >
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          // Headers
          h1: ({ children }) => (
            <h1 className="text-lg font-bold mb-2 text-gray-900 border-b border-gray-200 pb-1">
              {children}
            </h1>
          ),
          h2: ({ children }) => (
            <h2 className="text-base font-semibold mb-2 text-gray-800">
              {children}
            </h2>
          ),
          h3: ({ children }) => (
            <h3 className="text-sm font-medium mb-1 text-gray-700">
              {children}
            </h3>
          ),

          // Paragraphs
          p: ({ children }) => (
            <p className="mb-2 leading-relaxed last:mb-0">{children}</p>
          ),

          // Lists
          ul: ({ children }) => (
            <ul className="mb-2 ml-4 space-y-1 list-disc">{children}</ul>
          ),
          ol: ({ children }) => (
            <ol className="mb-2 ml-4 space-y-1 list-decimal">{children}</ol>
          ),
          li: ({ children }) => (
            <li className="text-sm leading-relaxed">{children}</li>
          ),

          // Emphasis
          strong: ({ children }) => (
            <strong className="font-semibold text-gray-900">{children}</strong>
          ),
          em: ({ children }) => (
            <em className="italic text-gray-700">{children}</em>
          ),

          code: ({ children }) => {
            return (
              <pre className="bg-gray-50 border border-gray-200 rounded-md p-3 mb-2 overflow-x-auto">
                <code className="text-xs font-mono text-gray-800">
                  {children}
                </code>
              </pre>
            );
          },

          // Tables
          table: ({ children }) => (
            <div className="mb-2 overflow-x-auto">
              <table className="min-w-full text-xs border border-gray-200 rounded">
                {children}
              </table>
            </div>
          ),
          thead: ({ children }) => (
            <thead className="bg-gray-50">{children}</thead>
          ),
          tbody: ({ children }) => (
            <tbody className="divide-y divide-gray-200">{children}</tbody>
          ),
          tr: ({ children }) => (
            <tr className="hover:bg-gray-50">{children}</tr>
          ),
          th: ({ children }) => (
            <th className="px-2 py-1 text-left font-medium text-gray-700 border-b">
              {children}
            </th>
          ),
          td: ({ children }) => (
            <td className="px-2 py-1 text-gray-600 border-b">{children}</td>
          ),

          // Blockquotes
          blockquote: ({ children }) => (
            <blockquote className="border-l-4 border-blue-200 pl-3 mb-2 italic text-gray-600 bg-blue-50 py-1">
              {children}
            </blockquote>
          ),

          // Horizontal rule
          hr: () => <hr className="my-3 border-gray-300" />,

          // Links
          a: ({ href, children }) => (
            <a
              href={href}
              className="text-blue-600 hover:text-blue-800 underline"
              target="_blank"
              rel="noopener noreferrer"
            >
              {children}
            </a>
          ),
        }}
      >
        {content}
      </ReactMarkdown>
    </div>
  );
};

export default function CallAnalyticsDashboard() {
  const [calls, setCalls] = useState<CallOverviewData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [dataFields, setDataFields] = useState<string[]>([]);

  // Filter states - Updated with new date filtering options
  const [filters, setFilters] = useState<FilterState>({
    search: "",
    agent: "",
    queue: "",
    disposition: "",
    campaign: "",
    dateRange: "all",
    specificDate: "",
    dateFilterType: 'range',
  });

  // Chatbot states
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [showSampleQueries, setShowSampleQueries] = useState(true);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Fetch all transcribed calls and analyse data structure
  useEffect(() => {
    const fetchAllCalls = async () => {
      setLoading(true);
      setError(null);

      try {
        const response = await fetch("/api/supabase/get-all-transcriptions");

        if (!response.ok) {
          throw new Error(`Failed to fetch calls: ${response.statusText}`);
        }

        const data = await response.json();
        if (data.success) {
          setCalls(data.data);

          // Analyse data structure to understand all available fields
          if (data.data.length > 0) {
            const allFields = new Set<string>();
            data.data.forEach((call: CallOverviewData) => {
              Object.keys(call).forEach((key) => allFields.add(key));
            });
            setDataFields(Array.from(allFields).sort());
            console.log("Available data fields:", Array.from(allFields));
            console.log("Sample call data:", data.data[0]);
          }
        } else {
          setError(data.error || "Failed to load call data");
        }
      } catch (err) {
        console.error("Error fetching calls:", err);
        setError("Network error occurred while fetching call data");
      } finally {
        setLoading(false);
      }
    };

    fetchAllCalls();
  }, []);

  // Helper function to parse call duration from JSONB string
  const parseCallDuration = (durationJson: string | object): { minutes: number; seconds: number } => {
    try {
      let duration;
      if (typeof durationJson === 'string') {
        duration = JSON.parse(durationJson);
      } else {
        duration = durationJson;
      }
      
      return {
        minutes: duration.minutes || 0,
        seconds: duration.seconds || 0
      };
    } catch {
      return { minutes: 0, seconds: 0 };
    }
  };

  // Helper function to count sentiments
  const countSentiments = (sentimentJson: string | null): SentimentCounts => {
    if (!sentimentJson) return { positive: 0, neutral: 0, negative: 0 };

    try {
      const sentiments = JSON.parse(sentimentJson);
      return sentiments.reduce(
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (counts: SentimentCounts, item: any) => {
          const sentiment = item.sentiment.toLowerCase();
          if (sentiment === "positive") counts.positive++;
          else if (sentiment === "negative") counts.negative++;
          else counts.neutral++;
          return counts;
        },
        { positive: 0, neutral: 0, negative: 0 }
      );
    } catch {
      return { positive: 0, neutral: 0, negative: 0 };
    }
  };

  // Helper function to check if a date is the same day
  const isSameDay = (date1: Date, date2: Date): boolean => {
    return (
      date1.getFullYear() === date2.getFullYear() &&
      date1.getMonth() === date2.getMonth() &&
      date1.getDate() === date2.getDate()
    );
  };

  // Get unique values for filter dropdowns
  const uniqueAgents = useMemo(
    () =>
      [...new Set(calls.map((call) => call.agent_username))]
        .filter(Boolean)
        .sort(),
    [calls]
  );

  const uniqueQueues = useMemo(
    () =>
      [...new Set(calls.map((call) => call.queue_name))].filter(Boolean).sort(),
    [calls]
  );

  const uniqueDispositions = useMemo(
    () =>
      [...new Set(calls.map((call) => call.disposition_title))]
        .filter(Boolean)
        .sort(),
    [calls]
  );

  const uniqueCampaigns = useMemo(
    () =>
      [...new Set(calls.map((call) => call.campaign_name))]
        .filter(Boolean)
        .sort(),
    [calls]
  );

  // Filter calls based on current filters - Updated with new date filtering logic
  const filteredCalls = useMemo(() => {
    return calls.filter((call) => {
      // Search filter - now searches across more fields including transcript
      if (filters.search) {
        const searchTerm = filters.search.toLowerCase();
        const searchableText = [
          call.contact_id,
          call.agent_username,
          call.customer_cli,
          call.call_summary,
          call.queue_name,
          call.disposition_title,
          call.campaign_name,
          call.transcript_text,
          call.keywords,
          call.topics,
          call.primary_category,
          call.categories,
          call.resolution_status,
          call.escalation_reason,
        ]
          .filter(Boolean)
          .join(" ")
          .toLowerCase();

        if (!searchableText.includes(searchTerm)) return false;
      }

      // Agent filter
      if (filters.agent && call.agent_username !== filters.agent) return false;

      // Queue filter
      if (filters.queue && call.queue_name !== filters.queue) return false;

      // Disposition filter
      if (filters.disposition && call.disposition_title !== filters.disposition)
        return false;

      // Campaign filter
      if (filters.campaign && call.campaign_name !== filters.campaign)
        return false;

      // Updated Date filtering logic
      if (filters.dateFilterType === 'specific' && filters.specificDate) {
        const callDate = new Date(call.initiation_timestamp);
        const filterDate = new Date(filters.specificDate);
        if (!isSameDay(callDate, filterDate)) return false;
      } else if (filters.dateFilterType === 'range' && filters.dateRange !== "all") {
        const callDate = new Date(call.initiation_timestamp);
        const now = new Date();
        const daysDiff = Math.floor(
          (now.getTime() - callDate.getTime()) / (1000 * 60 * 60 * 24)
        );

        switch (filters.dateRange) {
          case "today":
            if (daysDiff > 0) return false;
            break;
          case "week":
            if (daysDiff > 7) return false;
            break;
          case "month":
            if (daysDiff > 30) return false;
            break;
          case "3months":
            if (daysDiff > 90) return false;
            break;
        }
      }

      return true;
    });
  }, [calls, filters]);

  // Analytics data processing
  const analyticsData = useMemo(() => {
    // Daily call volume
    const dailyVolume = filteredCalls.reduce((acc, call) => {
      const date = new Date(call.initiation_timestamp)
        .toISOString()
        .split("T")[0];
      acc[date] = (acc[date] || 0) + 1;
      return acc;
    }, {} as Record<string, number>);

    const dailyVolumeData = Object.entries(dailyVolume)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([date, count]) => ({
        date: new Date(date).toLocaleDateString(),
        calls: count,
      }));

    // Hourly volume in 15-minute intervals
    const hourlyVolume = filteredCalls.reduce((acc, call) => {
      const date = new Date(call.initiation_timestamp);
      const hour = date.getHours();
      const minute = date.getMinutes();
      const interval = Math.floor(minute / 15) * 15;
      const timeKey = `${hour.toString().padStart(2, "0")}:${interval
        .toString()
        .padStart(2, "0")}`;
      acc[timeKey] = (acc[timeKey] || 0) + 1;
      return acc;
    }, {} as Record<string, number>);

    const hourlyVolumeData = Array.from({ length: 96 }, (_, i) => {
      const hour = Math.floor(i / 4);
      const minute = (i % 4) * 15;
      const timeKey = `${hour.toString().padStart(2, "0")}:${minute
        .toString()
        .padStart(2, "0")}`;
      return {
        time: timeKey,
        calls: hourlyVolume[timeKey] || 0,
      };
    }).filter((item) => item.calls > 0);

    // Sentiment distribution
    const totalSentiments = filteredCalls.reduce(
      (acc, call) => {
        const sentiments = countSentiments(call.sentiment_analysis!);
        return {
          positive: acc.positive + sentiments.positive,
          neutral: acc.neutral + sentiments.neutral,
          negative: acc.negative + sentiments.negative,
        };
      },
      { positive: 0, neutral: 0, negative: 0 }
    );

    const sentimentData = [
      {
        name: "Positive",
        value: totalSentiments.positive,
        color: SENTIMENT_COLORS.positive,
      },
      {
        name: "Neutral",
        value: totalSentiments.neutral,
        color: SENTIMENT_COLORS.neutral,
      },
      {
        name: "Negative",
        value: totalSentiments.negative,
        color: SENTIMENT_COLORS.negative,
      },
    ].filter((item) => item.value > 0);
    
// Disposition breakdown
const dispositionBreakdown = filteredCalls.reduce((acc, call) => {
  const disposition = call.disposition_title || "Unknown";
  acc[disposition] = (acc[disposition] || 0) + 1;
  return acc;
}, {} as Record<string, number>);

const dispositionData = Object.entries(dispositionBreakdown)
  .sort(([, a], [, b]) => b - a) // Sort by count descending
  .slice(0, 10) // Limit to top 10 dispositions
  .map(([disposition, count]) => ({
    disposition: disposition.length > 25 ? disposition.substring(0, 25) + "..." : disposition,
    fullDisposition: disposition,
    count,
    percentage: Math.round((count / filteredCalls.length) * 100),
  }));

    // AHT by hour - Fixed calculation with proper JSONB parsing
    const ahtByHour = filteredCalls.reduce((acc, call) => {
      const hour = new Date(call.initiation_timestamp).getHours();
      // Parse call duration from JSONB format and convert to total seconds
      const duration = parseCallDuration(call.call_duration);
      const totalSeconds = duration.minutes * 60 + duration.seconds;

      if (!acc[hour]) {
        acc[hour] = { totalDuration: 0, callCount: 0 };
      }
      acc[hour].totalDuration += totalSeconds;
      acc[hour].callCount += 1;
      return acc;
    }, {} as Record<number, { totalDuration: number; callCount: number }>);

    const ahtData = Array.from({ length: 24 }, (_, hour) => {
      const data = ahtByHour[hour];
      const avgSeconds = data ? data.totalDuration / data.callCount : 0;
      return {
        hour: `${hour.toString().padStart(2, "0")}:00`,
        aht: Math.round(avgSeconds), // Display as seconds
        calls: data?.callCount || 0,
      };
    }).filter((item) => item.calls > 0);

    return {
      dailyVolumeData,
      hourlyVolumeData,
      sentimentData,
      ahtData,
      totalSentiments,
      dispositionData,
    };
  }, [filteredCalls]);

  // Summary statistics with enhanced metrics - Updated with New Leads Generated
const stats = useMemo(() => {
  const totalCalls = filteredCalls.length;
  
  // Calculate total duration in seconds for accurate AHT using JSONB parsing
  const totalDurationSeconds = filteredCalls.reduce((sum, call) => {
    const duration = parseCallDuration(call.call_duration);
    return sum + (duration.minutes * 60) + duration.seconds;
  }, 0);

  // Calculate average duration and format as seconds only
  const avgDurationSeconds = totalCalls > 0 ? totalDurationSeconds / totalCalls : 0;

  // Additional stats
  const callsWithTranscripts = filteredCalls.filter(
    (call) => call.transcript_text && call.transcript_text.trim().length > 0
  ).length;
  const transcriptCoverage =
    totalCalls > 0
      ? Math.round((callsWithTranscripts / totalCalls) * 100)
      : 0;

  // NEW: Count leads generated based on specific disposition
  const newLeadsGenerated = filteredCalls.filter(
    (call) => call.disposition_title === "Conversation - Lead Generated: New Business"
  ).length;

  // Calculate lead generation rate from total calls
  const leadGenerationRate = totalCalls > 0 ? Math.round((newLeadsGenerated / totalCalls) * 100) : 0;

  return {
    totalCalls,
    avgDuration: Math.round(avgDurationSeconds), // Display as seconds only
    totalSentiments: analyticsData.totalSentiments,
    transcriptCoverage,
    callsWithTranscripts,
    newLeadsGenerated,
    leadGenerationRate,
  };
}, [filteredCalls, analyticsData.totalSentiments]);

  const resetFilters = () => {
    setFilters({
      search: "",
      agent: "",
      queue: "",
      disposition: "",
      campaign: "",
      dateRange: "all",
      specificDate: "",
      dateFilterType: 'range',
    });
  };

  // Helper function to get current date in YYYY-MM-DD format for date input
  const getCurrentDate = () => {
    return new Date().toISOString().split('T')[0];
  };

  // Helper function to format the filter display text
  const getDateFilterDisplay = () => {
    if (filters.dateFilterType === 'specific' && filters.specificDate) {
      return `Date: ${new Date(filters.specificDate).toLocaleDateString()}`;
    } else if (filters.dateFilterType === 'range' && filters.dateRange !== 'all') {
      const rangeLabels = {
        today: 'Today',
        week: 'Last 7 Days',
        month: 'Last 30 Days',
        '3months': 'Last 3 Months'
      };
      return `Range: ${rangeLabels[filters.dateRange as keyof typeof rangeLabels] || filters.dateRange}`;
    }
    return 'All Time';
  };

  // Chatbot functions
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  // Initialize chatbot with enhanced welcome message
  useEffect(() => {
    if (filteredCalls.length > 0 && messages.length === 0) {
      const welcomeMessage: Message = {
        id: Date.now().toString(),
        role: "assistant",
        content: `# TSAi Assistant Ready

I've loaded **${filteredCalls.length}** call records with comprehensive data including:

• **${stats.callsWithTranscripts}** calls with full transcripts (${stats.transcriptCoverage}% coverage)
• **${dataFields.length}** data fields per call
• **${uniqueAgents.length}** agents across **${uniqueQueues.length}** queues

## What can I help you with?

Feel free to ask me about:
- **Call volumes and trends**
- **Agent performance comparisons**
- **Sentiment analysis insights**
- **Disposition breakdowns**
- **Transcript content analysis**

Let's dive into your data!`,
        timestamp: new Date(),
      };
      setMessages([welcomeMessage]);
    }
  }, [
    filteredCalls,
    messages.length,
    dataFields.length,
    stats,
    uniqueAgents.length,
    uniqueQueues.length,
  ]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      role: "user",
      content: input.trim(),
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInput("");
    setIsLoading(true);
    setShowSampleQueries(false);

    try {
      const response = await fetch("/api/openAI/ai-query", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          message: input.trim(),
          transcriptionData: filteredCalls, // Send complete filtered data including transcript_text
          conversationHistory: messages.slice(-6), // Keep more context
          dataFields: dataFields, // Send available field information
        }),
      });

      if (!response.ok) {
        throw new Error("Failed to get AI response");
      }

      const data = await response.json();

      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content: data.response,
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error) {
      console.error("Error getting AI response:", error);

      const errorMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content:
          "Sorry, I encountered an error while processing your request. Please try again or rephrase your question.",
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const clearChat = () => {
    setMessages([]);
    setShowSampleQueries(true);
  };

  const handleSampleQuery = (query: string) => {
    setInput(query);
    setShowSampleQueries(false);
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-bg-primary flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full mx-auto mb-4"></div>
          <p className="text-gray-600">Loading call analytics...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center max-w-md">
          <div className="bg-red-50 border border-red-200 rounded-lg p-6">
            <h2 className="text-xl font-semibold text-red-800 mb-2">
              Error Loading Call Data
            </h2>
            <p className="text-red-600 mb-4">{error}</p>
            <div className="space-x-3">
              <button
                onClick={() => window.location.reload()}
                className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors"
              >
                Retry
              </button>
              <Link
                href="/tge"
                className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors inline-block"
              >
                Back to Dashboard
              </Link>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="h-screen bg-bg-primary text-black flex flex-col">
      {/* Header - Fixed */}
      <div className="bg-white shadow-sm border-b border-bg-secondary flex-shrink-0">
        <div className="max-w-full mx-auto px-4 sm:px-6 lg:px-8 bg-bg-secondary">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center space-x-4">
              <Link
                href="/tge"
                className="text-blue-600 hover:text-blue-800 font-medium text-2xl"
              >
                <BiSolidHome />
              </Link>
              <div className="h-6 w-px bg-gray-300"></div>
              <h1 className="text-xl font-semibold text-white">
                Call Analytics Dashboard
              </h1>
            </div>
            <div className="text-sm text-gray-500">
              {stats.totalCalls} calls • {stats.transcriptCoverage}% with
              transcripts • {getDateFilterDisplay()}
            </div>
          </div>
        </div>
      </div>

      {/* Main Container - Flexible Layout */}
      <div className="flex-1 flex overflow-hidden">
        {/* Main Content Area - Left Side */}
        <div className="flex-1 flex flex-col">
          <div className="px-4 sm:px-6 lg:px-8 py-4 flex-shrink-0">
            {/* Enhanced Search Bar with Updated Date Filtering */}
            <div className="bg-bg-secondary rounded-lg shadow-sm border p-4 mb-4 text-bg-secondary">
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-white font-medium">Search & Filter</h3>
                <button
                  onClick={resetFilters}
                  className="px-2 py-1 text-xs bg-gray-100 hover:bg-gray-200 text-gray-700 rounded transition-colors"
                >
                  Reset
                </button>
              </div>

              {/* Search and Filters - Updated with new date controls */}
              <div className="space-y-3">
                {/* First Row: Search and basic filters */}
                <div className="flex gap-3 items-end">
                  {/* Global Search - Takes up more space */}
                  <div className="flex-[2] min-w-0">
                    <label className="block text-xs font-bold text-white mb-1">
                      Search
                    </label>
                    <input
                      type="text"
                      placeholder="Search across calls, transcripts, categories, agents..."
                      value={filters.search}
                      onChange={(e) =>
                        setFilters((prev) => ({
                          ...prev,
                          search: e.target.value,
                        }))
                      }
                      className="w-full px-3 py-2 border border-white text-white rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>

                  {/* Agent Filter */}
                  <div className="flex-1 min-w-0">
                    <label className="block text-xs font-bold text-white mb-1">
                      Agent
                    </label>
                    <select
                      value={filters.agent}
                      onChange={(e) =>
                        setFilters((prev) => ({ ...prev, agent: e.target.value }))
                      }
                      className="w-full px-2 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500 text-white"
                    >
                      <option value="" className="bg-bg-primary text-white">
                        All Agents
                      </option>
                      {uniqueAgents.map((agent) => (
                        <option
                          key={agent}
                          value={agent}
                          className="bg-bg-primary text-white"
                        >
                          {agent}
                        </option>
                      ))}
                    </select>
                  </div>

                  {/* Queue Filter */}
                  <div className="flex-1 min-w-0">
                    <label className="block text-xs font-bold text-white mb-1">
                      Queue
                    </label>
                    <select
                      value={filters.queue}
                      onChange={(e) =>
                        setFilters((prev) => ({ ...prev, queue: e.target.value }))
                      }
                      className="w-full px-2 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500 text-white"
                    >
                      <option value="" className="text-white bg-bg-primary">
                        All Queues
                      </option>
                      {uniqueQueues.map((queue) => (
                        <option
                          key={queue}
                          value={queue}
                          className="text-white bg-bg-primary"
                        >
                          {queue}
                        </option>
                      ))}
                    </select>
                  </div>

                  {/* Disposition Filter */}
                  <div className="flex-1 min-w-0">
                    <label className="block text-xs font-bold text-white mb-1">
                      Disposition
                    </label>
                    <select
                      value={filters.disposition}
                      onChange={(e) =>
                        setFilters((prev) => ({
                          ...prev,
                          disposition: e.target.value,
                        }))
                      }
                      className="w-full px-2 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500 text-white"
                    >
                      <option value="" className="text-white bg-bg-primary">
                        All Dispositions
                      </option>
                      {uniqueDispositions.map((disposition) => (
                        <option
                          key={disposition}
                          value={disposition}
                          className="text-white bg-bg-primary"
                        >
                          {disposition}
                        </option>
                      ))}
                    </select>
                  </div>

                  {/* Campaign Filter */}
                  <div className="flex-1 min-w-0">
                    <label className="block text-xs font-bold text-white mb-1">
                      Campaign
                    </label>
                    <select
                      value={filters.campaign}
                      onChange={(e) =>
                        setFilters((prev) => ({
                          ...prev,
                          campaign: e.target.value,
                        }))
                      }
                      className="w-full px-2 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500 text-white"
                    >
                      <option value="" className="text-white bg-bg-primary">
                        All Campaigns
                      </option>
                      {uniqueCampaigns.map((campaign) => (
                        <option
                          key={campaign}
                          value={campaign}
                          className="text-white bg-bg-primary"
                        >
                          {campaign}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                {/* Second Row: Enhanced Date Filtering */}
                <div className="flex gap-3 items-end border-t border-gray-600 pt-3">
                  {/* Date Filter Type Toggle */}
                  <div className="flex-shrink-0">
                    <label className="block text-xs font-bold text-white mb-1">
                      Date Filter
                    </label>
                    <div className="flex rounded-lg overflow-hidden border border-gray-300">
                      <button
                        type="button"
                        onClick={() => setFilters(prev => ({ 
                          ...prev, 
                          dateFilterType: 'range',
                          specificDate: '' 
                        }))}
                        className={`px-3 py-2 text-sm transition-colors ${
                          filters.dateFilterType === 'range' 
                            ? 'bg-blue-600 text-white' 
                            : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                        }`}
                      >
                        Range
                      </button>
                      <button
                        type="button"
                        onClick={() => setFilters(prev => ({ 
                          ...prev, 
                          dateFilterType: 'specific',
                          dateRange: 'all' 
                        }))}
                        className={`px-3 py-2 text-sm transition-colors ${
                          filters.dateFilterType === 'specific' 
                            ? 'bg-blue-600 text-white' 
                            : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                        }`}
                      >
                        Specific
                      </button>
                    </div>
                  </div>

                  {/* Conditional Date Controls */}
                  {filters.dateFilterType === 'range' ? (
                    <div className="flex-1">
                      <label className="block text-xs font-bold text-white mb-1">
                        Date Range
                      </label>
                      <select
                        value={filters.dateRange}
                        onChange={(e) =>
                          setFilters((prev) => ({
                            ...prev,
                            dateRange: e.target.value,
                          }))
                        }
                        className="w-full px-2 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500 text-white"
                      >
                        <option value="all" className="text-white bg-bg-primary">
                          All Time
                        </option>
                        <option value="today" className="text-white bg-bg-primary">
                          Today
                        </option>
                        <option value="week" className="text-white bg-bg-primary">
                          Last 7 Days
                        </option>
                        <option value="month" className="text-white bg-bg-primary">
                          Last 30 Days
                        </option>
                        <option
                          value="3months"
                          className="text-white bg-bg-primary"
                        >
                          Last 3 Months
                        </option>
                      </select>
                    </div>
                  ) : (
                    <div className="flex-1">
                      <label className="block text-xs font-bold text-white mb-1">
                        Specific Date
                      </label>
                      <input
                        type="date"
                        value={filters.specificDate}
                        onChange={(e) =>
                          setFilters((prev) => ({
                            ...prev,
                            specificDate: e.target.value,
                          }))
                        }
                        max={getCurrentDate()}
                        className="w-full px-2 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500 text-white"
                      />
                    </div>
                  )}

                  {/* Quick Date Buttons for Specific Date */}
                  {filters.dateFilterType === 'specific' && (
                    <div className="flex gap-2">
                      <button
                        type="button"
                        onClick={() => setFilters(prev => ({ 
                          ...prev, 
                          specificDate: getCurrentDate() 
                        }))}
                        className="px-3 py-2 text-xs bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors"
                      >
                        Today
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          const yesterday = new Date();
                          yesterday.setDate(yesterday.getDate() - 1);
                          setFilters(prev => ({ 
                            ...prev, 
                            specificDate: yesterday.toISOString().split('T')[0] 
                          }));
                        }}
                        className="px-3 py-2 text-xs bg-gray-600 text-white rounded hover:bg-gray-700 transition-colors"
                      >
                        Yesterday
                      </button>
                    </div>
                  )}
                </div>
              </div>
            </div>

            {/* Enhanced Summary Statistics */}
            <div className="grid grid-cols-2 md:grid-cols-6 gap-4">
              <div className="flex bg-bg-secondary rounded-lg shadow-sm border border-bg-secondary p-4">
                <div className="flex-1">
                  <h3 className="text-sm font-medium text-white">
                    Total Calls
                  </h3>
                  <p className="text-2xl font-bold text-blue-600">
                    {stats.totalCalls}
                  </p>
                </div>
                <div className="flex-1 ">
                  <ResponsiveContainer width="100%" height={100}>
                    <BarChart data={analyticsData.dailyVolumeData}>
                      <CartesianGrid stroke="#272B2F" strokeDasharray="3 3" />
                      <XAxis dataKey="time" stroke="none" />
                      {/* <YAxis /> */}
                      <Bar dataKey="calls" fill="#10B981" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
              <div className="flex bg-bg-secondary rounded-lg shadow-sm border border-bg-secondary p-4">
                <div className="flex-1">
                  <h3 className="text-sm font-medium text-white">
                    Avg Handle Time (sec)
                  </h3>
                  <p className="text-2xl font-bold text-blue-600">
                    {stats.avgDuration}
                  </p>
                </div>
                <div className="flex-1">
                  <ResponsiveContainer width="100%" height={100}>
                    <LineChart data={analyticsData.ahtData}>
                      <CartesianGrid stroke="#272B2F" strokeDasharray="3 3" />
                      <XAxis dataKey="hour" stroke="none" />
                      <Line
                        type="monotone"
                        dataKey="aht"
                        stroke="#EF4444"
                        strokeWidth={2}
                        dot={false}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
              <div className="bg-bg-secondary rounded-lg shadow-sm border border-bg-secondary p-4">
                <h3 className="text-sm font-medium text-white">Transcripts</h3>
                <p className="text-2xl font-bold text-blue-600">
                  {stats.transcriptCoverage}%
                </p>
                <p className="text-xs text-gray-500">
                  {stats.callsWithTranscripts} calls
                </p>
              </div>
              <div className="bg-bg-secondary rounded-lg shadow-sm border border-bg-secondary p-4">
                <h3 className="text-sm font-medium text-white">
                  Active Agents
                </h3>
                <p className="text-2xl font-bold text-blue-600">
                  {uniqueAgents.length}
                </p>
              </div>
              <div className="bg-bg-secondary rounded-lg shadow-sm border border-bg-secondary p-4">
                <h3 className="text-sm font-medium text-white">
                  Sentiment Score
                </h3>
                <div className="flex gap-2 mt-2">
                  <span className="text-sm text-green-600">
                    Pos: {stats.totalSentiments.positive}
                  </span>
                  <span className="text-sm text-gray-200">
                    Neu: {stats.totalSentiments.neutral}
                  </span>
                  <span className="text-sm text-red-600">
                    Neg: {stats.totalSentiments.negative}
                  </span>
                </div>
              </div>
              <div className="bg-bg-secondary rounded-lg shadow-sm border border-bg-secondary p-4">
                <h3 className="text-sm font-medium text-white">
                  New Leads Generated
                </h3>
                <p className="text-2xl font-bold text-blue-600">
                  {stats.newLeadsGenerated}
                </p>
                <p className="text-xs text-gray-400 mt-1">
                  {stats.leadGenerationRate}% conversion rate
                </p>
              </div>
            </div>
          </div>

          {/* Analytics Charts Section - Scrollable */}
          <div className="flex-1 px-4 sm:px-6 lg:px-8 pb-4 overflow-y-auto">
            <div className="space-y-6">
              {/* Analytics Charts */}
              <div className="grid grid-cols-1 lg:grid-cols-1">
                <div className="w-full flex gap-x-6">
                  {/* Daily Call Volume */}
                  <div className="flex-1 bg-bg-secondary rounded-lg shadow-sm border border-bg-secondary drop-shadow p-6">
                    <h3 className="text-xs font-semibold text-white mb-4">
                      Daily Call Volume
                    </h3>
                    <ResponsiveContainer width="100%" height={300}>
                      <AreaChart data={analyticsData.dailyVolumeData}>
                        <CartesianGrid stroke="#272B2F" strokeDasharray="3 3" />
                        <XAxis dataKey="date" stroke="none" />
                        {/* <YAxis /> */}
                        <Tooltip contentStyle={{ backgroundColor: "black", color: "white", border: "none" }}/>
                        <Area
                          type="monotone"
                          dataKey="calls"
                          stroke="#3B82F6"
                          fill="#3B82F6"
                          fillOpacity={0.1}
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>

                  <div className="flex-1 bg-bg-secondary rounded-lg shadow-sm border border-bg-secondary drop-shadow p-6">
                    <h3 className="text-xs font-semibold text-white mb-4">
                      Average Handle Time by Hour (seconds)
                    </h3>
                    <ResponsiveContainer width="100%" height={300}>
                      <LineChart data={analyticsData.ahtData}>
                        <CartesianGrid stroke="#272B2F" strokeDasharray="3 3" />
                        <XAxis dataKey="hour" stroke="none" />
                        {/* <YAxis label={{ value: 'Minutes', angle: -90, position: 'insideLeft' }} /> */}
                        <Tooltip
                          formatter={(value) => [`${value} sec`, "AHT"]}
                           contentStyle={{ backgroundColor: "black", color: "white", border: "none" }}
                        />
                        <Line
                          type="monotone"
                          dataKey="aht"
                          stroke="#EF4444"
                          strokeWidth={2}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                  {/* Hourly Call Volume */}
                  <div className="flex-1 bg-bg-secondary rounded-lg shadow-sm border border-bg-secondary drop-shadow p-6">
                    <h3 className="text-xs font-semibold text-white mb-4">
                      Call Volume by Time (15min intervals)
                    </h3>
                    <ResponsiveContainer width="100%" height={300}>
                      <BarChart data={analyticsData.hourlyVolumeData}>
                        <CartesianGrid stroke="#272B2F" strokeDasharray="3 3" />
                        <XAxis dataKey="time" stroke="none" />
                        {/* <YAxis /> */}
                        <Tooltip  contentStyle={{ backgroundColor: "black", color: "white", border: "none" }}/>
                        <Bar dataKey="calls" fill="#10B981" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                  {/* Sentiment Distribution */}
                  <div className="flex-1 bg-bg-secondary rounded-lg shadow-sm border border-bg-secondary drop-shadow p-6">
                    <h3 className="text-xs font-semibold text-white mb-4">
                      Sentiment Distribution
                    </h3>
                    <ResponsiveContainer width="100%" height={300}>
                      <PieChart>
                        <Pie
                          data={analyticsData.sentimentData}
                          cx="50%"
                          cy="50%"
                          outerRadius={80}
                          fill="#8884d8"
                          dataKey="value"
                          label={({ name, value }) => `${name}: ${value}`}
                        >
                          {analyticsData.sentimentData.map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={entry.color} />
                          ))}
                        </Pie>
                        <Tooltip  contentStyle={{ backgroundColor: "black", border: "none", color: "white" }}/>
                        <Legend />
                      </PieChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>

            <div className="grid grid-cols-1 lg:grid-cols-1 gap-6">
  {/* Call Disposition Breakdown - Full Width */}
  <div className="bg-bg-secondary rounded-lg shadow-sm border border-bg-secondary drop-shadow p-6">
    <h3 className="text-lg font-semibold text-white mb-4">
      Call Disposition Breakdown
    </h3>
    <ResponsiveContainer width="100%" height={400}>
      <BarChart
        data={analyticsData.dispositionData}
        margin={{ top: 20, right: 30, left: 20, bottom: 60 }}
      >
        <CartesianGrid stroke="#272B2F" strokeDasharray="3 3" />
        <XAxis
          dataKey="disposition"
          angle={-45}
          textAnchor="end"
          height={80}
          interval={0}
          fontSize={10}
        />
        <YAxis />
        <Tooltip
          contentStyle={{ backgroundColor: "black", color: "white", border: "none" }}
          formatter={(value) => [value, "Call Count"]}
          labelFormatter={(label) => {
            const item = analyticsData.dispositionData.find(
              (d) => d.disposition === label
            );
            return item
              ? `${item.fullDisposition} (${item.percentage}%)`
              : label;
          }}
        />
        <Bar dataKey="count" fill="#F59E0B" />
      </BarChart>
    </ResponsiveContainer>
  </div>
</div>
            </div>
          </div>
        </div>

        {/* Enhanced AI Chatbot Sidebar - Fixed Right Side */}
        <div className="w-96 flex flex-col bg-bg-secondary border-l border-bg-secondary h-full">
          {/* Header */}
          <div className="p-4 border-b border-bg-secondary flex-shrink-0">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold text-white">
                TSAi Assistant
              </h3>
              <div className="flex items-center gap-3">
                <div className="text-xs text-gray-500">
                  {filteredCalls.length} calls
                </div>
                <button
                  onClick={clearChat}
                  className="px-2 py-1 text-xs bg-gray-100 hover:bg-gray-200 text-gray-600 rounded transition-colors"
                >
                  Clear
                </button>
              </div>
            </div>
          </div>

          {/* Sample Queries */}
          {showSampleQueries && messages.length <= 1 && (
            <div className="p-4 border-b border-bg-secondary bg-bg-primary flex-shrink-0">
              <h4 className="text-sm font-medium text-white mb-2">
                Try these queries:
              </h4>
              <div className="space-y-1">
                {SAMPLE_QUERIES.map((query, index) => (
                  <button
                    key={index}
                    onClick={() => handleSampleQuery(query)}
                    className="block w-full text-left text-xs text-gray-400 hover:text-blue-900 hover:bg-blue-100 px-2 py-1 rounded transition-colors"
                  >
                    &quot;{query}&quot;
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Messages with Markdown Rendering - Scrollable */}
          <div className="flex-1 overflow-y-auto p-4 space-y-3">
            {messages.map((message) => (
              <div
                key={message.id}
                className={`flex ${
                  message.role === "user" ? "justify-end" : "justify-start"
                }`}
              >
                <div
                  className={`max-w-[85%] p-3 rounded-lg text-sm ${
                    message.role === "user"
                      ? "bg-[#2c3554] text-white"
                      : "bg-[#58b88a] text-gray-900"
                  }`}
                >
                  <MarkdownMessage
                    content={message.content}
                    isUser={message.role === "user"}
                  />
                  <div
                    className={`text-xs mt-2 ${
                      message.role === "user" ? "text-blue-100" : "text-white"
                    }`}
                  >
                    {message.timestamp.toLocaleTimeString()}
                  </div>
                </div>
              </div>
            ))}

            {isLoading && (
              <div className="flex justify-start">
                <div className="bg-gray-100 p-3 rounded-lg text-sm">
                  <div className="flex items-center space-x-2">
                    <div className="animate-pulse">Thinking...</div>
                    <div className="flex space-x-1">
                      <div className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce"></div>
                      <div
                        className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce"
                        style={{ animationDelay: "0.1s" }}
                      ></div>
                      <div
                        className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce"
                        style={{ animationDelay: "0.2s" }}
                      ></div>
                    </div>
                  </div>
                </div>
              </div>
            )}

            <div ref={messagesEndRef} />
          </div>

          {/* Enhanced Input - Fixed Bottom */}
          <div className="p-4 border-t border-gray-200 flex-shrink-0">
            <form onSubmit={handleSubmit} className="space-y-2">
              <div className="flex gap-2">
                <input
                  type="text"
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  placeholder="Ask about counts, transcripts, trends..."
                  className="flex-1 px-3 py-2 text-sm border border-white text-white rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  disabled={isLoading}
                />
                <button
                  type="submit"
                  disabled={isLoading || !input.trim()}
                  className="px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                >
                  {isLoading ? "..." : "Send"}
                </button>
              </div>
              <div className="text-xs text-gray-500">
                💡 Supports rich formatting and tables
              </div>
            </form>
          </div>
        </div>
      </div>
    </div>
  );
}
"use client";

import { useState, useEffect, useRef } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { BiSolidHome } from "react-icons/bi";
// import ReactMarkdown from 'react-markdown';
// import remarkGfm from 'remark-gfm';

// ... (keep all existing interfaces - CallTranscriptionData, ParsedSpeakerData, etc.)

interface CallTranscriptionData {
  contact_id: string;
  recording_location: string;
  transcript_text: string
  queue_name?: string;
  agent_username: string;
  initiation_timestamp: string;
  speaker_data?: string;
  sentiment_analysis?: string;
  entities?: string;
  categories?: string;
  disposition_title?: string;
  call_summary?: string;
  campaign_name?: string;
  campaign_id?: string;
  customer_cli?: string;
  agent_hold_time?: number;
  total_hold_time?: {
    minutes: number | 0,
    seconds: number | 0
  };
  time_in_queue?: number;
  created_at?: string;
  updated_at?: string;
  call_duration: {
    minutes: number | 0,
    seconds: number | 0
  };
}

interface ParsedSpeakerData {
  speaker: string;
  text: string;
  start: number;
  end: number;
  confidence: number;
}

interface ParsedSentimentData {
  text: string;
  sentiment: string;
  confidence: number;
}

interface ParsedEntity {
  entity_type: string;
  text: string;
  start: number;
  end: number;
  confidence: number;
}

interface ParsedCategory {
  category: string;
  confidence: number;
}

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
}

// Custom Markdown Components for styling
// const MarkdownComponents = {
//   // Headers
//   h1: ({ children }: { children: React.ReactNode }) => (
//     <h1 className="text-xl font-bold mb-3 text-gray-900">{children}</h1>
//   ),
//   h2: ({ children }: { children: React.ReactNode }) => (
//     <h2 className="text-lg font-semibold mb-2 text-gray-900">{children}</h2>
//   ),
//   h3: ({ children }: { children: React.ReactNode }) => (
//     <h3 className="text-md font-medium mb-2 text-gray-900">{children}</h3>
//   ),
  
//   // Paragraphs
//   p: ({ children }: { children: React.ReactNode }) => (
//     <p className="mb-2 last:mb-0 leading-relaxed">{children}</p>
//   ),
  
//   // Lists
//   ul: ({ children }: { children: React.ReactNode }) => (
//     <ul className="list-disc list-inside mb-2 space-y-1">{children}</ul>
//   ),
//   ol: ({ children }: { children: React.ReactNode }) => (
//     <ol className="list-decimal list-inside mb-2 space-y-1">{children}</ol>
//   ),
//   li: ({ children }: { children: React.ReactNode }) => (
//     <li className="ml-2">{children}</li>
//   ),
  
//   // Emphasis
//   strong: ({ children }: { children: React.ReactNode }) => (
//     <strong className="font-semibold text-gray-900">{children}</strong>
//   ),
//   em: ({ children }: { children: React.ReactNode }) => (
//     <em className="italic text-gray-800">{children}</em>
//   ),
  
//   // Code
//   code: ({ children, className }: { children: React.ReactNode; className?: string }) => {
//     const isInline = !className?.includes('language-');
    
//     if (isInline) {
//       return (
//         <code className="bg-gray-100 text-gray-800 px-1 py-0.5 rounded text-sm font-mono">
//           {children}
//         </code>
//       );
//     }
    
//     return (
//       <pre className="bg-gray-100 text-gray-800 p-3 rounded-lg overflow-x-auto mb-2">
//         <code className="text-sm font-mono">{children}</code>
//       </pre>
//     );
//   },
  
//   // Blockquotes
//   blockquote: ({ children }: { children: React.ReactNode }) => (
//     <blockquote className="border-l-4 border-gray-300 pl-4 italic text-gray-700 mb-2">
//       {children}
//     </blockquote>
//   ),
  
//   // Links
//   a: ({ href, children }: { href?: string; children: React.ReactNode }) => (
//     <a 
//       href={href} 
//       className="text-blue-600 hover:text-blue-800 underline"
//       target="_blank"
//       rel="noopener noreferrer"
//     >
//       {children}
//     </a>
//   ),
  
//   // Tables
//   table: ({ children }: { children: React.ReactNode }) => (
//     <div className="overflow-x-auto mb-2">
//       <table className="min-w-full border border-gray-300 rounded-lg">
//         {children}
//       </table>
//     </div>
//   ),
//   th: ({ children }: { children: React.ReactNode }) => (
//     <th className="border border-gray-300 px-3 py-2 bg-gray-100 font-semibold text-left">
//       {children}
//     </th>
//   ),
//   td: ({ children }: { children: React.ReactNode }) => (
//     <td className="border border-gray-300 px-3 py-2">{children}</td>
//   ),
  
//   // Horizontal rule
//   hr: () => <hr className="my-4 border-gray-300" />,
// };

// AI Chat Component with Markdown Rendering
const AIChat = ({ callData }: { callData: CallTranscriptionData }) => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  // Initialize with welcome message
  useEffect(() => {
    const welcomeMessage: Message = {
      id: Date.now().toString(),
      role: 'assistant',
      content: `I'm here to help you analyse this call. I have access to:

• **The full transcript** and speaker data
• **Sentiment analysis** results
• **Extracted entities** and categories
• **Call metadata** (duration, agent, disposition, etc.)
• **AI-generated summary**

You can ask me questions like:
- "What was the main issue discussed?"
- "How did the customer feel during the call?"
- "Summarise the key points"
- "What was the agent's performance?"

What would you like to know about this call?`,
      timestamp: new Date()
    };
    
    setMessages([welcomeMessage]);
  }, [callData.contact_id]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      role: 'user',
      content: input.trim(),
      timestamp: new Date()
    };

    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);

    try {
      const response = await fetch('/api/openAI/ai-query-single-call', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          message: input.trim(),
          callData: callData,
          conversationHistory: messages.slice(-10) // Send last 10 messages for context
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to get AI response');
      }

      const data = await response.json();
      
      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: data.response,
        timestamp: new Date()
      };

      setMessages(prev => [...prev, assistantMessage]);
    } catch (error) {
      console.error('Error getting AI response:', error);
      
      const errorMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: 'Sorry, I encountered an error while processing your request. Please try again.',
        timestamp: new Date()
      };

      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const clearChat = () => {
    const welcomeMessage: Message = {
      id: Date.now().toString(),
      role: 'assistant',
      content: `Chat cleared! I'm still here to help you analyse this call. What would you like to know?`,
      timestamp: new Date()
    };
    setMessages([welcomeMessage]);
  };

  return (
    <div className="bg-bg-secondary shadow-sm border border-border flex flex-col h-full">
      {/* Header */}
      <div className="p-4 border-b border-border ">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-white">TSAi Assistant</h3>
          <button
            onClick={clearChat}
            className="px-3 py-1 text-sm bg-gray-100 hover:bg-gray-200 text-gray-700 rounded transition-colors"
          >
            Clear
          </button>
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.map((message) => (
          <div
            key={message.id}
            className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}
          >
            <div
              className={`max-w-[80%] p-3 rounded-lg ${
                message.role === 'user'
                      ? "bg-[#2c3554] text-white"
                      : "bg-[#58b88a] text-gray-900"
              }`}
            >
              {/* Render markdown for assistant messages, plain text for user messages */}
              {message.role === 'assistant' ? (
                <div className="prose prose-sm max-w-none">
                    {message.content}
                  {/* <ReactMarkdown 
                    components={MarkdownComponents}
                    remarkPlugins={[remarkGfm]}
                  >
                    {message.content}
                  </ReactMarkdown> */}
                </div>
              ) : (
                <div className="whitespace-pre-wrap">{message.content}</div>
              )}
              
              <div className={`text-xs mt-2 ${
                message.role === 'user' ? 'text-blue-100' : 'text-white'
              }`}>
                {message.timestamp.toLocaleTimeString()}
              </div>
            </div>
          </div>
        ))}
        
        {isLoading && (
          <div className="flex justify-start">
            <div className="bg-gray-100 text-gray-900 p-3 rounded-lg">
              <div className="flex items-center space-x-2">
                <div className="animate-pulse">Thinking...</div>
                <div className="flex space-x-1">
                  <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce"></div>
                  <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0.1s' }}></div>
                  <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0.2s' }}></div>
                </div>
              </div>
            </div>
          </div>
        )}
        
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="p-4 border-t border-gray-200">
        <form onSubmit={handleSubmit} className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask me about this call..."
            className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            disabled={isLoading}
          />
          <button
            type="submit"
            disabled={isLoading || !input.trim()}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {isLoading ? 'Sending...' : 'Send'}
          </button>
        </form>
      </div>
    </div>
  );
};

// ... (keep all the other components like AudioPlayer, etc. exactly the same)

// Audio Player Component (unchanged)
const AudioPlayer = ({
  recordingLocation,
  contactId,
}: {
  recordingLocation: string;
  contactId: string;
}) => {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [audioUrl, setAudioUrl] = useState<string | null>(null);
  const [isPlaying, setIsPlaying] = useState(false);
  const [currentTime, setCurrentTime] = useState(0);
  const [duration, setDuration] = useState(0);
  const audioRef = useRef<HTMLAudioElement>(null);

  const loadAudio = async () => {
    if (audioUrl) return;

    setIsLoading(true);
    setError(null);

    try {
      const params = new URLSearchParams({
        recordingLocation,
        contactId,
      });

      const response = await fetch(`/api/sftp/audio?${params}`);

      if (!response.ok) {
        throw new Error(`Failed to load audio: ${response.statusText}`);
      }

      const audioBlob = await response.blob();
      const url = URL.createObjectURL(audioBlob);
      setAudioUrl(url);
    } catch (err) {
      console.error("Error loading audio:", err);
      setError(err instanceof Error ? err.message : "Failed to load audio");
    } finally {
      setIsLoading(false);
    }
  };

  const togglePlay = () => {
    if (!audioRef.current) return;
    if (isPlaying) {
      audioRef.current.pause();
    } else {
      audioRef.current.play();
    }
  };

  const handleTimeUpdate = () => {
    if (audioRef.current) {
      setCurrentTime(audioRef.current.currentTime);
    }
  };

  const handleLoadedMetadata = () => {
    if (audioRef.current) {
      setDuration(audioRef.current.duration);
    }
  };

  const handleSeek = (e: React.ChangeEvent<HTMLInputElement>) => {
    const time = parseFloat(e.target.value);
    if (audioRef.current) {
      audioRef.current.currentTime = time;
      setCurrentTime(time);
    }
  };

  const formatTime = (seconds: number) => {
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins}:${secs.toString().padStart(2, "0")}`;
  };

  useEffect(() => {
    return () => {
      if (audioUrl) {
        URL.revokeObjectURL(audioUrl);
      }
    };
  }, [audioUrl]);

  return (
    <div className="bg-bg-secondary border border-border rounded-lg p-4">
      <h3 className="text-lg font-semibold text-white mb-3">
        Call Recording
      </h3>

      {!audioUrl && !isLoading && !error && (
        <button
          onClick={loadAudio}
          className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          Load Audio
        </button>
      )}

      {isLoading && (
        <div className="flex items-center gap-3">
          <div className="animate-spin w-5 h-5 border-2 border-blue-500 border-t-transparent rounded-full"></div>
          <span className="text-gray-600">Loading audio...</span>
        </div>
      )}

      {error && (
        <div className="text-red-600 bg-red-50 p-3 rounded-lg">
          Error: {error}
          <button
            onClick={() => {
              setError(null);
              loadAudio();
            }}
            className="ml-3 text-blue-600 hover:text-blue-800 underline"
          >
            Retry
          </button>
        </div>
      )}

      {audioUrl && (
        <div className="space-y-3">
          <audio
            ref={audioRef}
            src={audioUrl}
            onTimeUpdate={handleTimeUpdate}
            onLoadedMetadata={handleLoadedMetadata}
            onPlay={() => setIsPlaying(true)}
            onPause={() => setIsPlaying(false)}
            onEnded={() => setIsPlaying(false)}
            preload="metadata"
          />

          <div className="flex items-center gap-3">
            <button
              onClick={togglePlay}
              className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
            >
              {isPlaying ? "⏸️ Pause" : "▶️ Play"}
            </button>

            <span className="text-sm text-gray-600 min-w-fit">
              {formatTime(currentTime)}
            </span>

            <input
              type="range"
              min={0}
              max={duration || 0}
              value={currentTime}
              onChange={handleSeek}
              className="flex-1 h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer"
            />

            <span className="text-sm text-gray-600 min-w-fit">
              {formatTime(duration)}
            </span>
          </div>
        </div>
      )}
    </div>
  );
};

// ... (rest of the component stays exactly the same)
export default function CallDetailPage() {
  const params = useParams();
  const contactId = params.contact_id as string;

  const [callData, setCallData] = useState<CallTranscriptionData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<
    "overview" | "transcript" | "analysis"
  >("overview");

  useEffect(() => {
    const fetchCallData = async () => {
      if (!contactId) return;

      setLoading(true);
      setError(null);

      try {
        const response = await fetch(
          `/api/supabase/save-transcription?contact_id=${contactId}`
        );

        if (!response.ok) {
          if (response.status === 404) {
            setError(
              "Call data not found. This call may not have been transcribed yet."
            );
          } else {
            throw new Error(
              `Failed to fetch call data: ${response.statusText}`
            );
          }
          return;
        }

        const data = await response.json();
        if (data.success) {
          setCallData(data.data);
        } else {
          setError(data.error || "Failed to load call data");
        }
      } catch (err) {
        console.error("Error fetching call data:", err);
        setError("Network error occurred while fetching call data");
      } finally {
        setLoading(false);
      }
    };

    fetchCallData();
  }, [contactId]);

  const formatTimestamp = (timestamp: string) => {
    return new Date(timestamp).toLocaleString();
  };

  const parseJsonField = (jsonString: string | null | undefined) => {
    if (!jsonString) return null;
    try {
      return JSON.parse(jsonString);
    } catch {
      return null;
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-bg-primary flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full mx-auto mb-4"></div>
          <p className="text-gray-600">Loading call details...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-bg-primary flex items-center justify-center">
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

  if (!callData) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <p className="text-gray-600">No call data found.</p>
        </div>
      </div>
    );
  }

  const parsedSpeakerData: ParsedSpeakerData[] =
    parseJsonField(callData.speaker_data) || [];
  const parsedSentimentData: ParsedSentimentData[] =
    parseJsonField(callData.sentiment_analysis) || [];
  const parsedEntities: ParsedEntity[] =
    parseJsonField(callData.entities) || [];
  const parsedCategories: ParsedCategory[] =
    parseJsonField(callData.categories) || [];

function countSentiments(jsonString: string) {
  if (jsonString) {
  const sentiments = JSON.parse(jsonString);
  
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return sentiments.reduce((counts: any, item: any) => {
    counts[item.sentiment.toLowerCase()]++;
    console.log(item.sentiment)
    return counts;
  }, { positive: 0, neutral: 0, negative: 0 });}
  else return { positive: 0, neutral: 0, negative: 0 }
}

  return (
    <div className="min-h-screen bg-bg-primary">
      {/* Header */}
      <div className="bg-bg-secondary shadow-sm border-b border-border">
        <div className="max-w-full mx-auto px-4 sm:px-6 lg:px-8">
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
                Call Details - {contactId}
              </h1>
            </div>
            <div className="text-sm text-gray-500">
              {formatTimestamp(callData.initiation_timestamp)}
            </div>
          </div>
        </div>
      </div>

      {/* Main Layout - Split into left content and right AI chat */}
      <div className="flex h-[calc(100vh-64px)]">
        {/* Left Side - Main Content */}
        <div className="flex-1 overflow-y-auto ">
          <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            {/* Tab Navigation */}
            <div className="mb-6">
              <nav className="flex space-x-8">
                {[
                  { id: "overview", label: "Overview" },
                  { id: "transcript", label: "Transcript" },
                  { id: "analysis", label: "Analysis" },
                ].map((tab) => (
                  <button
                    key={tab.id}
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                    onClick={() => setActiveTab(tab.id as any)}
                    className={`py-2 px-1 border-b-2 font-medium text-sm ${
                      activeTab === tab.id
                        ? "border-blue-500 text-blue-600"
                        : "border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300"
                    }`}
                  >
                    {tab.label}
                  </button>
                ))}
              </nav>
            </div>

            {/* Tab Content - Overview */}
            {activeTab === "overview" && (
              <div className="space-y-6 h-[calc(100vh-64px)]">
                {/* Call Summary Card */}
                <div className="bg-bg-secondary rounded-lg shadow-sm border border-border p-6">
                  <h2 className="text-xl font-semibold text-white mb-4">
                    Call Summary
                  </h2>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                    <div>
                      <label className="text-sm font-medium text-gray-400">
                        Agent
                      </label>
                      <p className="text-lg text-white">
                        {callData.agent_username}
                      </p>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-400">
                        Queue
                      </label>
                      <p className="text-lg text-white">
                        {callData.queue_name || "N/A"}
                      </p>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-400">
                        Disposition
                      </label>
                      <p className="text-lg text-white">
                        {callData.disposition_title || "N/A"}
                      </p>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-400">
                        Campaign
                      </label>
                      <p className="text-lg text-white">
                        {callData.campaign_name || "N/A"}
                      </p>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-400">
                        Customer CLI
                      </label>
                      <p className="text-lg text-white">
                        {callData.customer_cli || "N/A"}
                      </p>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-400">
                        Total Call Duration
                      </label>
                      <p className="text-lg text-white">
                        {callData.call_duration?.minutes === undefined ? '0' : callData.call_duration?.minutes}m {callData.call_duration?.seconds === undefined ? '0' : callData.call_duration?.seconds}s
                      </p>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-400">
                        Total Hold Time
                      </label>
                      <p className="text-lg text-white">
                        {callData.total_hold_time?.minutes === undefined ? '0' : callData.total_hold_time?.minutes}m {callData.total_hold_time?.seconds === undefined ? '0' : callData.total_hold_time?.seconds}s
                      </p>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-400">
                        Sentiment
                      </label>
                      <p className="text-lg text-white">
                        <span className="text-green-400">{countSentiments(callData.sentiment_analysis!).positive}</span>/
                        <span className="text-gray-400">{countSentiments(callData.sentiment_analysis!).neutral}</span>/
                        <span className="text-red-400">{countSentiments(callData.sentiment_analysis!).negative}</span>
                      </p>
                    </div>
                  </div>
                </div>

                {/* Call Summary Text */}
                {callData.call_summary && (
                  <div className="bg-bg-secondary rounded-lg shadow-sm border border-border p-6">
                    <h3 className="text-lg font-semibold text-white mb-3">
                      AI Summary
                    </h3>
                    <p className="text-gray-400 leading-relaxed">
                      {callData.call_summary}
                    </p>
                  </div>
                )}

                {/* Audio Player */}
                {callData.recording_location && (
                  <AudioPlayer
                    recordingLocation={callData.recording_location}
                    contactId={callData.contact_id}
                  />
                )}
              </div>
            )}

            {/* Rest of the tabs remain the same... */}
            {activeTab === "transcript" && (
              <div className="bg-bg-secondary rounded-lg shadow-sm border border-border">
                <div className="p-6">
                  <h2 className="text-xl font-semibold text-white mb-4">
                    Full Transcript
                  </h2>

                  {parsedSpeakerData.length > 0 ? (
                    <div
                      className="space-y-4 max-h-[calc(100vh-400px)] overflow-y-auto [&::-webkit-scrollbar]:w-2
    [&::-webkit-scrollbar-track]:rounded-full
    [&::-webkit-scrollbar-track]:bg-gray-100
    [&::-webkit-scrollbar-thumb]:rounded-full
    [&::-webkit-scrollbar-thumb]:bg-gray-300
    dark:[&::-webkit-scrollbar-track]:bg-neutral-700
    dark:[&::-webkit-scrollbar-thumb]:bg-neutral-500"
                    >
                      {parsedSpeakerData.map((utterance, index) => (
                        <div
                          key={index}
                          className="border-l-4 border-blue-200 pl-4 py-2 bg-bg-primary"
                        >
                          <div className="flex items-center gap-2 mb-1">
                            <span className="font-semibold text-blue-600">
                              Speaker {utterance.speaker}
                            </span>
                            <span className="text-xs text-gray-400">
                              {Math.floor(utterance.start / 1000)}s -{" "}
                              {Math.floor(utterance.end / 1000)}s
                            </span>
                          </div>
                          <p className="text-white">{utterance.text}</p>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="bg-gray-50 rounded-lg p-4">
                      <p className="text-gray-700 whitespace-pre-wrap">
                        {callData.transcript_text || "No transcript available"}
                      </p>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Analysis tab remains the same... */}
            {activeTab === "analysis" && (
              <div className="space-y-6 max-h-[calc(100vh-400px)] overflow-y-auto [&::-webkit-scrollbar]:w-2
    [&::-webkit-scrollbar-track]:rounded-full
    [&::-webkit-scrollbar-track]:bg-gray-100
    [&::-webkit-scrollbar-thumb]:rounded-full
    [&::-webkit-scrollbar-thumb]:bg-gray-300
    dark:[&::-webkit-scrollbar-track]:bg-neutral-700
    dark:[&::-webkit-scrollbar-thumb]:bg-neutral-500">

                {/* Categories */}
                {parsedCategories.length > 0 && (
                  <div className="bg-bg-secondary rounded-lg shadow-sm border border-border p-6">
                    <h3 className="text-lg font-semibold text-white mb-4">
                      Categories
                    </h3>
                    <div className="flex flex-wrap gap-2">
                        <span
                          className="px-3 py-1 bg-purple-100 text-purple-800 rounded-full text-sm font-medium"
                        >
                          {callData.categories}
                        </span>
                    </div>
                  </div>
                )}
                {/* Sentiment Analysis */}
                {parsedSentimentData.length > 0 && (
                  <div className="bg-bg-secondary rounded-lg shadow-sm border border-border p-6">
                    <h3 className="text-lg font-semibold text-white mb-4">
                      Sentiment Analysis
                    </h3>
                    <div
                      className="space-y-3 max-h-[calc(100vh-500px)] overflow-y-auto [&::-webkit-scrollbar]:w-2
    [&::-webkit-scrollbar-track]:rounded-full
    [&::-webkit-scrollbar-track]:bg-gray-100
    [&::-webkit-scrollbar-thumb]:rounded-full
    [&::-webkit-scrollbar-thumb]:bg-gray-300
    dark:[&::-webkit-scrollbar-track]:bg-neutral-700
    dark:[&::-webkit-scrollbar-thumb]:bg-neutral-500"
                    >
                      {parsedSentimentData.map((sentiment, index) => (
                        <div
                          key={index}
                          className="flex items-center justify-between p-3 bg-bg-primary rounded-lg"
                        >
                          <span className="text-white">{sentiment.text}</span>
                          <div className="flex items-center gap-2">
                            <span
                              className={`px-2 py-1 rounded-full text-xs font-medium ${
                                sentiment.sentiment === "POSITIVE"
                                  ? "bg-green-100 text-green-800"
                                  : sentiment.sentiment === "NEGATIVE"
                                  ? "bg-red-100 text-red-800"
                                  : "bg-gray-100 text-gray-800"
                              }`}
                            >
                              {sentiment.sentiment}
                            </span>
                            <span className="text-sm text-gray-600">
                              {Math.round(sentiment.confidence * 100)}%
                            </span>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Entities */}
                {parsedEntities.length > 0 && (
                  <div className="bg-bg-secondary rounded-lg shadow-sm border border-border p-6 space-y-4 max-h-[calc(100vh-500px)] overflow-y-auto [&::-webkit-scrollbar]:w-2
    [&::-webkit-scrollbar-track]:rounded-full
    [&::-webkit-scrollbar-track]:bg-gray-100
    [&::-webkit-scrollbar-thumb]:rounded-full
    [&::-webkit-scrollbar-thumb]:bg-gray-300
    dark:[&::-webkit-scrollbar-track]:bg-neutral-700
    dark:[&::-webkit-scrollbar-thumb]:bg-neutral-500">
                    <h3 className="text-lg font-semibold text-white mb-4">
                      Extracted Entities
                    </h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      {parsedEntities.map((entity, index) => (
                        <div key={index} className="p-3 bg-bg-primary rounded-lg">
                          <div className="flex items-center justify-between mb-1">
                            <span className="font-medium text-blue-600">
                              {entity.entity_type}
                            </span>
                          </div>
                          <p className="text-white">&quot;{entity.text}&quot;</p>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        {/* Right Side - AI Chat Sidebar */}
        <div className="w-96 h-full">
          <AIChat callData={callData} />
        </div>
      </div>
    </div>
  );
}
/* eslint-disable @typescript-eslint/no-explicit-any */
import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;

const supabase = createClient(supabaseUrl, supabaseServiceKey);

// Define proper interfaces for type safety
interface CallDuration {
  minutes: number;
  seconds: number;
}

interface ProcessedSentiment {
  sentiment: string;
  score?: number;
  text?: string;
}

interface TranscriptProcessed {
  full: string;
  greeting: string;
  closing: string;
  length: number;
  wordCount: number;
  greetingSentiment: 'positive' | 'neutral' | 'negative' | null;
  closingSentiment: 'positive' | 'neutral' | 'negative' | null;
  hasTransfer: boolean;
  hasEscalation: boolean;
  keywordMatches: string[];
}

interface TransformedCallRecord {
  id?: string;
  contact_id: string;
  agent_username: string;
  initiation_timestamp: string;
  queue_name: string;
  disposition_title: string;
  campaign_name: string;
  customer_cli: string;
  call_duration: CallDuration;
  call_duration_total_seconds: number;
  call_summary: string;
  primary_category: string;
  categories: string;
  sentiment_analysis: string;
  processed_sentiments: ProcessedSentiment[] | null;
  transcript_text: string;
  transcript_processed: TranscriptProcessed | null;
  keywords: string;
  topics: string;
  satisfaction_score: number | null;
  resolution_status: string;
  escalation_reason: string;
  callback_requested: boolean;
  language: string;
  channel: string;
  created_at: string;
  updated_at: string;
  processed_at: string | null;
  has_transcript: boolean;
  has_sentiment: boolean;
  has_summary: boolean;
  is_short_call: boolean;
  is_long_call: boolean;
}

interface SentimentCount {
  positive: number;
  neutral: number;
  negative: number;
}

interface AgentStats {
  totalCalls: number;
  totalDuration: number;
  shortCalls: number;
  longCalls: number;
  sentimentCounts: SentimentCount;
  greetingStats: SentimentCount;
  closingStats: SentimentCount;
  categories: Record<string, number>;
  resolutionStats: Record<string, number>;
  callbackRequests: number;
  transferCount: number;
  escalationCount: number;
  avgSatisfactionScore: number;
  satisfactionScoreCount: number;
  // Calculated fields - make them required with default values
  avgDurationMinutes: number;
  shortCallRate: number;
  longCallRate: number;
  callbackRate: number;
  transferRate: number;
  escalationRate: number;
  positivePercentage: number;
  greetingPositiveRate: number;
  topCategory: string;
  positiveRate: number;
  positive: number;
  neutral: number;
  negative: number;
  total: number;
}

export async function GET() {
  try {
    console.log('Fetching and processing transcription data from Supabase...');

    // Fetch all transcription records with comprehensive field selection
    const { data, error } = await supabase
      .from('call_records')
      .select(`contact_id, transcript_text, queue_name, agent_username, initiation_timestamp, sentiment_analysis, categories, disposition_title, call_summary, call_duration, primary_category`)
      .order('initiation_timestamp', { ascending: false });

    if (error) {
      console.error('Supabase error:', error);
      return NextResponse.json(
        { 
          success: false, 
          error: 'Failed to fetch transcriptions from database',
          details: error.message 
        },
        { status: 500 }
      );
    }

    if (!data || data.length === 0) {
      return NextResponse.json({
        success: true,
        data: [],
        count: 0,
        analytics: null,
        timestamp: new Date().toISOString()
      });
    }

    // Transform and enhance data
    const transformedData = data.map(record => transformCallRecord(record));
    
    // Pre-process comprehensive analytics
    const analytics = await processComprehensiveAnalytics(transformedData);
    
    // Generate enhanced statistics
    const enhancedStatistics = generateEnhancedStatistics(transformedData, analytics);

    console.log('Data processing completed:', {
      totalRecords: transformedData.length,
      transcriptCoverage: enhancedStatistics.transcriptCoverage,
      analyticsGenerated: Object.keys(analytics).length
    });

    return NextResponse.json({
      success: true,
      data: transformedData,
      count: transformedData.length,
      analytics: analytics,
      statistics: enhancedStatistics,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('Error fetching and processing transcriptions:', error);
    
    let errorMessage = 'Internal server error';
    let errorDetails = '';
    
    if (error instanceof Error) {
      errorMessage = error.message;
      errorDetails = error.stack || '';
    }

    return NextResponse.json(
      { 
        success: false, 
        error: errorMessage,
        details: process.env.NODE_ENV === 'development' ? errorDetails : undefined,
        timestamp: new Date().toISOString()
      },
      { status: 500 }
    );
  }
}

function transformCallRecord(record: any): TransformedCallRecord {
  // Enhanced call duration processing
  let call_duration: CallDuration = { minutes: 0, seconds: 0 };
  
  if (record.call_duration) {
    if (typeof record.call_duration === 'object' && record.call_duration !== null) {
      call_duration = {
        minutes: Number(record.call_duration.minutes) || 0,
        seconds: Number(record.call_duration.seconds) || 0
      };
    } else if (typeof record.call_duration === 'number') {
      call_duration = {
        minutes: Math.floor(record.call_duration / 60),
        seconds: record.call_duration % 60
      };
    } else if (typeof record.call_duration === 'string') {
      const parts = record.call_duration.split(':');
      if (parts.length === 2) {
        call_duration = {
          minutes: parseInt(parts[0]) || 0,
          seconds: parseInt(parts[1]) || 0
        };
      }
    }
  }

  // Enhanced sentiment processing
  let processedSentiments: ProcessedSentiment[] | null = null;
  if (record.sentiment_analysis) {
    try {
      processedSentiments = JSON.parse(record.sentiment_analysis);
    } catch {
      console.warn('Invalid sentiment data for call:', record.contact_id);
    }
  }

  // Enhanced transcript processing
  const processedTranscript: TranscriptProcessed | null = record.transcript_text ? {
    full: record.transcript_text,
    greeting: extractGreeting(record.transcript_text),
    closing: extractClosing(record.transcript_text),
    length: record.transcript_text.length,
    wordCount: record.transcript_text.split(/\s+/).length,
    greetingSentiment: analyzeGreetingSentiment(record.transcript_text),
    closingSentiment: analyzeClosingSentiment(record.transcript_text),
    hasTransfer: checkForTransfer(record.transcript_text),
    hasEscalation: checkForEscalation(record.transcript_text),
    keywordMatches: extractKeywordMatches(record.transcript_text)
  } : null;

  return {
    // Core identification
    id: record.id,
    contact_id: record.contact_id?.toString() || '',
    agent_username: record.agent_username || '',
    initiation_timestamp: record.initiation_timestamp || record.created_at || new Date().toISOString(),
    
    // Call routing and categorization
    queue_name: record.queue_name || '',
    disposition_title: record.disposition_title || '',
    campaign_name: record.campaign_name || '',
    customer_cli: record.customer_cli || '',
    
    // Enhanced call metrics
    call_duration,
    call_duration_total_seconds: call_duration.minutes * 60 + call_duration.seconds,
    
    // Enhanced AI analysis
    call_summary: record.call_summary || '',
    primary_category: record.primary_category || '',
    categories: record.categories || '',
    sentiment_analysis: record.sentiment_analysis || '',
    processed_sentiments: processedSentiments,
    
    // Enhanced transcript data
    transcript_text: record.transcript_text || '',
    transcript_processed: processedTranscript,
    
    // Additional analysis fields
    keywords: record.keywords || '',
    topics: record.topics || '',
    satisfaction_score: record.satisfaction_score ? Number(record.satisfaction_score) : null,
    resolution_status: record.resolution_status || '',
    escalation_reason: record.escalation_reason || '',
    callback_requested: Boolean(record.callback_requested),
    language: record.language || '',
    channel: record.channel || '',
    
    // System fields
    created_at: record.created_at || new Date().toISOString(),
    updated_at: record.updated_at || record.created_at || new Date().toISOString(),
    processed_at: record.processed_at || null,
    
    // Analysis flags (computed)
    has_transcript: Boolean(record.transcript_text?.trim()),
    has_sentiment: Boolean(record.sentiment_analysis?.trim()),
    has_summary: Boolean(record.call_summary?.trim()),
    is_short_call: call_duration.minutes < 2,
    is_long_call: call_duration.minutes > 10,
  };
}

async function processComprehensiveAnalytics(data: TransformedCallRecord[]) {
  const analytics = {
    // Basic metrics
    totalCalls: data.length,
    dateRange: getDateRange(data),
    
    // Coverage metrics
    coverage: calculateCoverage(data),
    
    // Agent analytics
    agentAnalytics: processAgentAnalytics(data),
    
    // Sentiment analytics
    sentimentAnalytics: processSentimentAnalytics(data),
    
    // Greeting analytics
    greetingAnalytics: processGreetingAnalytics(data),
    
    // Category analytics
    categoryAnalytics: processCategoryAnalytics(data),
    
    // Time-based analytics
    timeAnalytics: processTimeAnalytics(data),
    
    // Transcript analytics
    transcriptAnalytics: processTranscriptAnalytics(data),
    
    // Performance analytics
    performanceAnalytics: processPerformanceAnalytics(data),
    
    // Quality analytics
    qualityAnalytics: processQualityAnalytics(data)
  };

  return analytics;
}

function getDateRange(data: TransformedCallRecord[]) {
  if (data.length === 0) return null;
  
  const dates = data.map(call => new Date(call.initiation_timestamp).getTime());
  return {
    earliest: new Date(Math.min(...dates)).toISOString(),
    latest: new Date(Math.max(...dates)).toISOString(),
    spanDays: Math.ceil((Math.max(...dates) - Math.min(...dates)) / (1000 * 60 * 60 * 24))
  };
}

function calculateCoverage(data: TransformedCallRecord[]) {
  const total = data.length;
  const withTranscripts = data.filter(call => call.has_transcript).length;
  const withSentiment = data.filter(call => call.has_sentiment).length;
  const withSummary = data.filter(call => call.has_summary).length;
  
  return {
    transcript: { count: withTranscripts, percentage: (withTranscripts / total) * 100 },
    sentiment: { count: withSentiment, percentage: (withSentiment / total) * 100 },
    summary: { count: withSummary, percentage: (withSummary / total) * 100 }
  };
}

function processAgentAnalytics(data: TransformedCallRecord[]) {
  const agentStats: Record<string, AgentStats> = {};
  
  data.forEach(call => {
    const agent = call.agent_username;
    if (!agent) return;
    
    if (!agentStats[agent]) {
      agentStats[agent] = {
        totalCalls: 0,
        totalDuration: 0,
        shortCalls: 0,
        longCalls: 0,
        sentimentCounts: { positive: 0, neutral: 0, negative: 0 },
        greetingStats: { positive: 0, neutral: 0, negative: 0 },
        closingStats: { positive: 0, neutral: 0, negative: 0 },
        categories: {},
        resolutionStats: {},
        callbackRequests: 0,
        transferCount: 0,
        escalationCount: 0,
        avgSatisfactionScore: 0,
        satisfactionScoreCount: 0,
        // Initialize all calculated fields with default values
        avgDurationMinutes: 0,
        shortCallRate: 0,
        longCallRate: 0,
        callbackRate: 0,
        transferRate: 0,
        escalationRate: 0,
        positivePercentage: 0,
        greetingPositiveRate: 0,
        topCategory: 'None',
        positiveRate: 0,
        positive: 0,
        neutral: 0,
        negative: 0,
        total: 0
      };
    }
    
    const stats = agentStats[agent];
    stats.totalCalls++;
    stats.totalDuration += call.call_duration_total_seconds;
    
    if (call.is_short_call) stats.shortCalls++;
    if (call.is_long_call) stats.longCalls++;
    
    // Process sentiments
    if (call.processed_sentiments) {
      call.processed_sentiments.forEach((item: ProcessedSentiment) => {
        const sentiment = item.sentiment?.toLowerCase();
        if (sentiment === 'positive') stats.sentimentCounts.positive++;
        else if (sentiment === 'negative') stats.sentimentCounts.negative++;
        else stats.sentimentCounts.neutral++;
      });
    }
    
    // Process transcript insights
    if (call.transcript_processed) {
      const greetingSentiment = call.transcript_processed.greetingSentiment;
      const closingSentiment = call.transcript_processed.closingSentiment;
      
      if (greetingSentiment) {
        stats.greetingStats[greetingSentiment]++;
        // Safely update the corresponding sentiment count
        if (greetingSentiment === 'positive') {
          stats.positive++;
        } else if (greetingSentiment === 'neutral') {
          stats.neutral++;
        } else if (greetingSentiment === 'negative') {
          stats.negative++;
        }
        stats.total++;
      }
      if (closingSentiment) stats.closingStats[closingSentiment]++;
      
      if (call.transcript_processed.hasTransfer) stats.transferCount++;
      if (call.transcript_processed.hasEscalation) stats.escalationCount++;
    }
    
    // Category tracking
    if (call.primary_category) {
      stats.categories[call.primary_category] = (stats.categories[call.primary_category] || 0) + 1;
    }
    
    // Resolution tracking
    if (call.resolution_status) {
      stats.resolutionStats[call.resolution_status] = (stats.resolutionStats[call.resolution_status] || 0) + 1;
    }
    
    // Satisfaction tracking
    if (call.satisfaction_score) {
      stats.avgSatisfactionScore = ((stats.avgSatisfactionScore * stats.satisfactionScoreCount) + call.satisfaction_score) / (stats.satisfactionScoreCount + 1);
      stats.satisfactionScoreCount++;
    }
    
    if (call.callback_requested) stats.callbackRequests++;
  });
  
  // Calculate derived metrics
  Object.keys(agentStats).forEach(agent => {
    const stats = agentStats[agent];
    stats.avgDurationMinutes = stats.totalDuration / stats.totalCalls / 60;
    stats.shortCallRate = (stats.shortCalls / stats.totalCalls) * 100;
    stats.longCallRate = (stats.longCalls / stats.totalCalls) * 100;
    stats.callbackRate = (stats.callbackRequests / stats.totalCalls) * 100;
    stats.transferRate = (stats.transferCount / stats.totalCalls) * 100;
    stats.escalationRate = (stats.escalationCount / stats.totalCalls) * 100;
    
    const totalSentiments = stats.sentimentCounts.positive + stats.sentimentCounts.neutral + stats.sentimentCounts.negative;
    stats.positivePercentage = totalSentiments > 0 ? (stats.sentimentCounts.positive / totalSentiments) * 100 : 0;
    
    const totalGreetings = stats.greetingStats.positive + stats.greetingStats.neutral + stats.greetingStats.negative;
    stats.greetingPositiveRate = totalGreetings > 0 ? (stats.greetingStats.positive / totalGreetings) * 100 : 0;
    
    // Calculate positiveRate for greeting stats
    stats.positiveRate = stats.total > 0 ? (stats.positive / stats.total) * 100 : 0;
    
    stats.topCategory = Object.entries(stats.categories).sort(([,a], [,b]) => (b as number) - (a as number))[0]?.[0] || 'None';
  });
  
  return agentStats;
}

function processSentimentAnalytics(data: TransformedCallRecord[]) {
  const analytics = {
    overall: { positive: 0, neutral: 0, negative: 0 },
    byCategory: {} as Record<string, SentimentCount>,
    byAgent: {} as Record<string, SentimentCount>,
    byTimeOfDay: {} as Record<string, SentimentCount>,
    greetingsSentiment: {} as Record<string, number>,
    closingsSentiment: {} as Record<string, number>,
    correlationWithDuration: { short: { positive: 0, negative: 0 }, long: { positive: 0, negative: 0 } }
  };
  
  data.forEach(call => {
    const category = call.primary_category || 'Uncategorized';
    const agent = call.agent_username;
    const hour = new Date(call.initiation_timestamp).getHours();
    const timeSlot = `${hour}:00`;
    
    // Initialize nested objects
    if (!analytics.byCategory[category]) {
      analytics.byCategory[category] = { positive: 0, neutral: 0, negative: 0 };
    }
    if (!analytics.byAgent[agent]) {
      analytics.byAgent[agent] = { positive: 0, neutral: 0, negative: 0 };
    }
    if (!analytics.byTimeOfDay[timeSlot]) {
      analytics.byTimeOfDay[timeSlot] = { positive: 0, neutral: 0, negative: 0 };
    }
    
    // Process overall sentiments
    if (call.processed_sentiments) {
      call.processed_sentiments.forEach((item: ProcessedSentiment) => {
        const sentiment = item.sentiment?.toLowerCase();
        if (sentiment === 'positive') {
          analytics.overall.positive++;
          analytics.byCategory[category].positive++;
          analytics.byAgent[agent].positive++;
          analytics.byTimeOfDay[timeSlot].positive++;
          
          if (call.is_short_call) analytics.correlationWithDuration.short.positive++;
          if (call.is_long_call) analytics.correlationWithDuration.long.positive++;
        } else if (sentiment === 'negative') {
          analytics.overall.negative++;
          analytics.byCategory[category].negative++;
          analytics.byAgent[agent].negative++;
          analytics.byTimeOfDay[timeSlot].negative++;
          
          if (call.is_short_call) analytics.correlationWithDuration.short.negative++;
          if (call.is_long_call) analytics.correlationWithDuration.long.negative++;
        } else {
          analytics.overall.neutral++;
          analytics.byCategory[category].neutral++;
          analytics.byAgent[agent].neutral++;
          analytics.byTimeOfDay[timeSlot].neutral++;
        }
      });
    }
    
    // Process greeting and closing sentiments
    if (call.transcript_processed) {
      const greetingSentiment = call.transcript_processed.greetingSentiment;
      const closingSentiment = call.transcript_processed.closingSentiment;
      
      if (greetingSentiment) {
        analytics.greetingsSentiment[greetingSentiment] = (analytics.greetingsSentiment[greetingSentiment] || 0) + 1;
      }
      
      if (closingSentiment) {
        analytics.closingsSentiment[closingSentiment] = (analytics.closingsSentiment[closingSentiment] || 0) + 1;
      }
    }
  });
  
  return analytics;
}

function processGreetingAnalytics(data: TransformedCallRecord[]) {
  const analytics = {
    totalAnalyzed: 0,
    sentimentDistribution: { positive: 0, neutral: 0, negative: 0 },
    byAgent: {} as Record<string, AgentStats>,
    commonPhrases: {} as Record<string, number>,
    correlationWithOverallSentiment: {
      positiveGreetingPositiveCall: 0,
      positiveGreetingNegativeCall: 0,
      negativeGreetingPositiveCall: 0,
      negativeGreetingNegativeCall: 0
    }
  };
  
  data.forEach(call => {
    if (!call.transcript_processed || !call.transcript_processed.greetingSentiment) return;
    
    analytics.totalAnalyzed++;
    const greetingSentiment = call.transcript_processed.greetingSentiment;
    const agent = call.agent_username;
    
    analytics.sentimentDistribution[greetingSentiment]++;
    
    if (!analytics.byAgent[agent]) {
      analytics.byAgent[agent] = {
        totalCalls: 0,
        totalDuration: 0,
        shortCalls: 0,
        longCalls: 0,
        sentimentCounts: { positive: 0, neutral: 0, negative: 0 },
        greetingStats: { positive: 0, neutral: 0, negative: 0 },
        closingStats: { positive: 0, neutral: 0, negative: 0 },
        categories: {},
        resolutionStats: {},
        callbackRequests: 0,
        transferCount: 0,
        escalationCount: 0,
        avgSatisfactionScore: 0,
        satisfactionScoreCount: 0,
        avgDurationMinutes: 0,
        shortCallRate: 0,
        longCallRate: 0,
        callbackRate: 0,
        transferRate: 0,
        escalationRate: 0,
        positivePercentage: 0,
        greetingPositiveRate: 0,
        topCategory: 'None',
        positive: 0,
        neutral: 0,
        negative: 0,
        total: 0,
        positiveRate: 0
      };
    }
    
    // Safely update the sentiment counts
    if (greetingSentiment === 'positive') {
      analytics.byAgent[agent].positive++;
    } else if (greetingSentiment === 'neutral') {
      analytics.byAgent[agent].neutral++;
    } else if (greetingSentiment === 'negative') {
      analytics.byAgent[agent].negative++;
    }
    analytics.byAgent[agent].total++;
    
    // Extract greeting phrases
    if (call.transcript_processed.greeting) {
      const greetingPhrase = extractMainGreetingPhrase(call.transcript_processed.greeting);
      if (greetingPhrase) {
        analytics.commonPhrases[greetingPhrase] = (analytics.commonPhrases[greetingPhrase] || 0) + 1;
      }
    }
    
    // Correlation analysis
    if (call.processed_sentiments && call.processed_sentiments.length > 0) {
      const hasPositiveOverall = call.processed_sentiments.some((s: ProcessedSentiment) => s.sentiment?.toLowerCase() === 'positive');
      const hasNegativeOverall = call.processed_sentiments.some((s: ProcessedSentiment) => s.sentiment?.toLowerCase() === 'negative');
      
      if (greetingSentiment === 'positive' && hasPositiveOverall) {
        analytics.correlationWithOverallSentiment.positiveGreetingPositiveCall++;
      } else if (greetingSentiment === 'positive' && hasNegativeOverall) {
        analytics.correlationWithOverallSentiment.positiveGreetingNegativeCall++;
      } else if (greetingSentiment === 'negative' && hasPositiveOverall) {
        analytics.correlationWithOverallSentiment.negativeGreetingPositiveCall++;
      } else if (greetingSentiment === 'negative' && hasNegativeOverall) {
        analytics.correlationWithOverallSentiment.negativeGreetingNegativeCall++;
      }
    }
  });
  
  // Calculate percentages for agents
  Object.keys(analytics.byAgent).forEach(agent => {
    const stats = analytics.byAgent[agent];
    if (stats && stats.total > 0) {
      stats.positiveRate = (stats.positive / stats.total) * 100;
    }
  });
  
  return analytics;
}

function processCategoryAnalytics(data: TransformedCallRecord[]) {
  const analytics = {
    distribution: {} as Record<string, number>,
    sentimentByCategory: {} as Record<string, SentimentCount>,
    avgDurationByCategory: {} as Record<string, number>,
    resolutionByCategory: {} as Record<string, Record<string, number>>
  };
  
  data.forEach(call => {
    const category = call.primary_category || 'Uncategorized';
    
    analytics.distribution[category] = (analytics.distribution[category] || 0) + 1;
    
    if (!analytics.sentimentByCategory[category]) {
      analytics.sentimentByCategory[category] = { positive: 0, neutral: 0, negative: 0 };
    }
    
    if (!analytics.avgDurationByCategory[category]) {
      analytics.avgDurationByCategory[category] = 0;
    }
    analytics.avgDurationByCategory[category] += call.call_duration_total_seconds;
    
    if (call.resolution_status) {
      if (!analytics.resolutionByCategory[category]) {
        analytics.resolutionByCategory[category] = {};
      }
      analytics.resolutionByCategory[category][call.resolution_status] = 
        (analytics.resolutionByCategory[category][call.resolution_status] || 0) + 1;
    }
    
    if (call.processed_sentiments) {
      call.processed_sentiments.forEach((item: ProcessedSentiment) => {
        const sentiment = item.sentiment?.toLowerCase();
        if (sentiment === 'positive') analytics.sentimentByCategory[category].positive++;
        else if (sentiment === 'negative') analytics.sentimentByCategory[category].negative++;
        else analytics.sentimentByCategory[category].neutral++;
      });
    }
  });
  
  // Calculate averages
  Object.keys(analytics.avgDurationByCategory).forEach(category => {
    analytics.avgDurationByCategory[category] = 
      analytics.avgDurationByCategory[category] / analytics.distribution[category] / 60; // Convert to minutes
  });
  
  return analytics;
}

function processTimeAnalytics(data: TransformedCallRecord[]) {
  const analytics = {
    hourlyVolume: {} as Record<number, number>,
    dailyVolume: {} as Record<string, number>,
    sentimentByHour: {} as Record<number, SentimentCount>,
    durationByHour: {} as Record<number, number[]>,
    peakHours: { volume: 0, sentiment: 0 }
  };
  
  data.forEach(call => {
    const date = new Date(call.initiation_timestamp);
    const hour = date.getHours();
    const dayKey = date.toISOString().split('T')[0];
    
    analytics.hourlyVolume[hour] = (analytics.hourlyVolume[hour] || 0) + 1;
    analytics.dailyVolume[dayKey] = (analytics.dailyVolume[dayKey] || 0) + 1;
    
    if (!analytics.sentimentByHour[hour]) {
      analytics.sentimentByHour[hour] = { positive: 0, neutral: 0, negative: 0 };
    }
    
    if (!analytics.durationByHour[hour]) {
      analytics.durationByHour[hour] = [];
    }
    analytics.durationByHour[hour].push(call.call_duration_total_seconds);
    
    if (call.processed_sentiments) {
      call.processed_sentiments.forEach((item: ProcessedSentiment) => {
        const sentiment = item.sentiment?.toLowerCase();
        if (sentiment === 'positive') analytics.sentimentByHour[hour].positive++;
        else if (sentiment === 'negative') analytics.sentimentByHour[hour].negative++;
        else analytics.sentimentByHour[hour].neutral++;
      });
    }
  });
  
  // Find peak hours
  analytics.peakHours.volume = parseInt(Object.entries(analytics.hourlyVolume)
    .sort(([,a], [,b]) => (b as number) - (a as number))[0]?.[0] || '0');
  
  analytics.peakHours.sentiment = parseInt(Object.entries(analytics.sentimentByHour)
    .sort(([,a], [,b]) => (b as SentimentCount).positive - (a as SentimentCount).positive)[0]?.[0] || '0');
  
  return analytics;
}

function processTranscriptAnalytics(data: TransformedCallRecord[]) {
  const callsWithTranscripts = data.filter(call => call.has_transcript);
  
  const analytics = {
    totalWithTranscripts: callsWithTranscripts.length,
    averageLength: 0,
    averageWordCount: 0,
    transferMentions: 0,
    escalationMentions: 0,
    keywordFrequency: {} as Record<string, number>,
    commonPhrases: {} as Record<string, number>,
    transcriptQuality: {
      short: 0,    // < 100 characters
      medium: 0,   // 100-500 characters
      long: 0,     // > 500 characters
      veryLong: 0  // > 2000 characters
    }
  };
  
  let totalLength = 0;
  let totalWordCount = 0;
  
  callsWithTranscripts.forEach(call => {
    const transcript = call.transcript_processed;
    if (!transcript) return;
    
    totalLength += transcript.length;
    totalWordCount += transcript.wordCount;
    
    if (transcript.hasTransfer) analytics.transferMentions++;
    if (transcript.hasEscalation) analytics.escalationMentions++;
    
    // Quality classification
    if (transcript.length < 100) analytics.transcriptQuality.short++;
    else if (transcript.length < 500) analytics.transcriptQuality.medium++;
    else if (transcript.length < 2000) analytics.transcriptQuality.long++;
    else analytics.transcriptQuality.veryLong++;
    
    // Keyword frequency
    if (transcript.keywordMatches) {
      transcript.keywordMatches.forEach((keyword: string) => {
        analytics.keywordFrequency[keyword] = (analytics.keywordFrequency[keyword] || 0) + 1;
      });
    }
  });
  
  analytics.averageLength = callsWithTranscripts.length > 0 ? totalLength / callsWithTranscripts.length : 0;
  analytics.averageWordCount = callsWithTranscripts.length > 0 ? totalWordCount / callsWithTranscripts.length : 0;
  
  return analytics;
}

function processPerformanceAnalytics(data: TransformedCallRecord[]) {
  return {
    callVolume: {
      total: data.length,
      withTranscripts: data.filter(call => call.has_transcript).length,
      withSentiment: data.filter(call => call.has_sentiment).length
    },
    efficiency: {
      shortCalls: data.filter(call => call.is_short_call).length,
      longCalls: data.filter(call => call.is_long_call).length,
      avgDuration: data.length > 0 ? data.reduce((sum, call) => sum + call.call_duration_total_seconds, 0) / data.length / 60 : 0
    },
    outcomes: {
      callbackRequests: data.filter(call => call.callback_requested).length,
      escalations: data.filter(call => call.transcript_processed?.hasEscalation).length,
      transfers: data.filter(call => call.transcript_processed?.hasTransfer).length
    }
  };
}

function processQualityAnalytics(data: TransformedCallRecord[]) {
  const callsWithScores = data.filter(call => call.satisfaction_score !== null);
  
  return {
    satisfactionScore: {
      average: callsWithScores.length > 0 ? 
        callsWithScores.reduce((sum, call) => sum + (call.satisfaction_score || 0), 0) / callsWithScores.length : 0,
      distribution: callsWithScores.reduce((acc, call) => {
        const score = Math.floor(call.satisfaction_score || 0);
        acc[score] = (acc[score] || 0) + 1;
        return acc;
      }, {} as Record<number, number>)
    },
    sentimentQuality: calculateSentimentQuality(data),
    greetingQuality: calculateGreetingQuality(data)
  };
}

// Helper functions for transcript processing
function extractGreeting(transcript: string): string {
  return transcript.substring(0, 200);
}

function extractClosing(transcript: string): string {
  return transcript.substring(Math.max(0, transcript.length - 200));
}

function analyzeGreetingSentiment(transcript: string): 'positive' | 'neutral' | 'negative' | null {
  if (!transcript) return null;
  
  const greeting = transcript.substring(0, 200).toLowerCase();
  
  const positiveIndicators = [
    'thank you for calling', 'great to speak with you', 'wonderful', 'excellent',
    'happy to help', 'pleased to', 'delighted', 'fantastic', 'amazing',
    'good morning', 'good afternoon', 'how are you today', 'hope you\'re well'
  ];
  
  const negativeIndicators = [
    'sorry for the wait', 'apologize', 'unfortunately', 'problem', 'issue',
    'delay', 'trouble', 'difficult', 'frustrated', 'upset'
  ];
  
  const positiveCount = positiveIndicators.filter(indicator => greeting.includes(indicator)).length;
  const negativeCount = negativeIndicators.filter(indicator => greeting.includes(indicator)).length;
  
  if (positiveCount > negativeCount) return 'positive';
  if (negativeCount > positiveCount) return 'negative';
  return 'neutral';
}

function analyzeClosingSentiment(transcript: string): 'positive' | 'neutral' | 'negative' | null {
  if (!transcript) return null;
  
  const closing = transcript.substring(Math.max(0, transcript.length - 200)).toLowerCase();
  
  const positiveIndicators = [
    'thank you', 'have a great day', 'pleasure helping', 'take care',
    'wonderful speaking', 'happy to help', 'resolved', 'sorted out'
  ];
  
  const negativeIndicators = [
    'sorry', 'unfortunately', 'unable to', 'can\'t help', 'escalate',
    'callback', 'still not resolved', 'frustrating'
  ];
  
  const positiveCount = positiveIndicators.filter(indicator => closing.includes(indicator)).length;
  const negativeCount = negativeIndicators.filter(indicator => closing.includes(indicator)).length;
  
  if (positiveCount > negativeCount) return 'positive';
  if (negativeCount > positiveCount) return 'negative';
  return 'neutral';
}

function checkForTransfer(transcript: string): boolean {
  const indicators = ['transfer', 'transferring', 'put you through', 'connect you to', 'pass you to'];
  return indicators.some(indicator => transcript.toLowerCase().includes(indicator));
}

function checkForEscalation(transcript: string): boolean {
  const indicators = ['escalate', 'supervisor', 'manager', 'senior', 'complaint'];
  return indicators.some(indicator => transcript.toLowerCase().includes(indicator));
}

function extractKeywordMatches(transcript: string): string[] {
  const keywords = [
    'price', 'cost', 'discount', 'fee', 'payment', 'billing',
    'transfer', 'escalate', 'supervisor', 'manager',
    'problem', 'issue', 'complaint', 'frustrated',
    'happy', 'satisfied', 'pleased', 'great',
    'resolve', 'fix', 'solve', 'help'
  ];
  
  const lowerTranscript = transcript.toLowerCase();
  return keywords.filter(keyword => lowerTranscript.includes(keyword));
}

function extractMainGreetingPhrase(greeting: string): string | null {
  const patterns = [
    /thank you for calling \w+/i,
    /good (morning|afternoon|evening)/i,
    /how can I help you/i,
    /how may I assist/i
  ];
  
  for (const pattern of patterns) {
    const match = greeting.match(pattern);
    if (match) return match[0];
  }
  
  return null;
}

function calculateSentimentQuality(data: TransformedCallRecord[]) {
  const total = data.length;
  const withSentiment = data.filter(call => call.has_sentiment).length;
  const positiveGreetings = data.filter(call => 
    call.transcript_processed?.greetingSentiment === 'positive'
  ).length;
  
  return {
    coverage: total > 0 ? (withSentiment / total) * 100 : 0,
    positiveGreetingRate: total > 0 ? (positiveGreetings / total) * 100 : 0,
    overallPositivityRate: calculateOverallPositivityRate(data)
  };
}

function calculateGreetingQuality(data: TransformedCallRecord[]) {
  const withGreetingAnalysis = data.filter(call => 
    call.transcript_processed?.greetingSentiment
  );
  
  if (withGreetingAnalysis.length === 0) {
    return { averageScore: 0, distribution: {} };
  }
  
  const distribution = withGreetingAnalysis.reduce((acc, call) => {
    const sentiment = call.transcript_processed?.greetingSentiment;
    if (sentiment) {
      acc[sentiment] = (acc[sentiment] || 0) + 1;
    }
    return acc;
  }, {} as Record<string, number>);
  
  return {
    averageScore: withGreetingAnalysis.length > 0 ? (distribution.positive || 0) / withGreetingAnalysis.length : 0,
    distribution
  };
}

function calculateOverallPositivityRate(data: TransformedCallRecord[]): number {
  const withSentiment = data.filter(call => call.processed_sentiments);
  if (withSentiment.length === 0) return 0;
  
  let totalSentiments = 0;
  let positiveSentiments = 0;
  
  withSentiment.forEach(call => {
    call.processed_sentiments?.forEach((sentiment: ProcessedSentiment) => {
      totalSentiments++;
      if (sentiment.sentiment?.toLowerCase() === 'positive') {
        positiveSentiments++;
      }
    });
  });
  
  return totalSentiments > 0 ? (positiveSentiments / totalSentiments) * 100 : 0;
}

function generateEnhancedStatistics(data: TransformedCallRecord[], analytics: any) {
  const total = data.length;
  
  return {
    // Basic counts
    totalRecords: total,
    fieldsAvailable: data.length > 0 ? Object.keys(data[0]).length : 0,
    
    // Coverage statistics
    transcriptCoverage: analytics.coverage.transcript.percentage,
    sentimentCoverage: analytics.coverage.sentiment.percentage,
    summaryCoverage: analytics.coverage.summary.percentage,
    
    // Performance statistics
    averageCallDuration: analytics.performanceAnalytics.efficiency.avgDuration,
    shortCallPercentage: total > 0 ? (analytics.performanceAnalytics.efficiency.shortCalls / total) * 100 : 0,
    longCallPercentage: total > 0 ? (analytics.performanceAnalytics.efficiency.longCalls / total) * 100 : 0,
    
    // Quality statistics
    averageSatisfactionScore: analytics.qualityAnalytics.satisfactionScore.average,
    positiveGreetingPercentage: analytics.greetingAnalytics.totalAnalyzed > 0 ? (analytics.greetingAnalytics.sentimentDistribution.positive / analytics.greetingAnalytics.totalAnalyzed) * 100 : 0,
    overallPositivityRate: analytics.qualityAnalytics.sentimentQuality.overallPositivityRate,
    
    // Operational statistics
    uniqueAgents: Object.keys(analytics.agentAnalytics).length,
    uniqueCategories: Object.keys(analytics.categoryAnalytics.distribution).length,
    callbackRequestRate: total > 0 ? (analytics.performanceAnalytics.outcomes.callbackRequests / total) * 100 : 0,
    escalationRate: total > 0 ? (analytics.performanceAnalytics.outcomes.escalations / total) * 100 : 0,
    transferRate: total > 0 ? (analytics.performanceAnalytics.outcomes.transfers / total) * 100 : 0,
    
    // Time range
    dateRange: analytics.dateRange,
    
    // Data quality indicators
    dataQualityScore: calculateDataQualityScore(analytics)
  };
}

function calculateDataQualityScore(analytics: any): number {
  // Calculate a composite score based on data completeness and quality
  const transcriptScore = Math.min(analytics.coverage.transcript.percentage / 80, 1) * 30; // 30% weight
  const sentimentScore = Math.min(analytics.coverage.sentiment.percentage / 90, 1) * 25;   // 25% weight
  const summaryScore = Math.min(analytics.coverage.summary.percentage / 70, 1) * 20;       // 20% weight
  const diversityScore = Math.min(Object.keys(analytics.agentAnalytics).length / 10, 1) * 15; // 15% weight
  const volumeScore = Math.min(analytics.totalCalls / 1000, 1) * 10;                       // 10% weight
  
  return Math.round(transcriptScore + sentimentScore + summaryScore + diversityScore + volumeScore);
}

// Optional: Enhanced filtering endpoint
export async function POST(request: NextRequest) {
  try {
    const { filters, includeAnalytics } = await request.json();
    
    let query = supabase
      .from('call_records')
      .select('*');

    // Apply filters
    if (filters) {
      if (filters.agent_username) {
        query = query.eq('agent_username', filters.agent_username);
      }
      if (filters.queue_name) {
        query = query.eq('queue_name', filters.queue_name);
      }
      if (filters.primary_category) {
        query = query.eq('primary_category', filters.primary_category);
      }
      if (filters.date_from) {
        query = query.gte('initiation_timestamp', filters.date_from);
      }
      if (filters.date_to) {
        query = query.lte('initiation_timestamp', filters.date_to);
      }
      if (filters.has_transcript) {
        query = query.not('transcript_text', 'is', null);
      }
      if (filters.sentiment_type) {
        // This would require more complex filtering based on sentiment analysis
        // Could be implemented with additional database views or processing
      }
    }

    const { data, error } = await query.order('initiation_timestamp', { ascending: false });

    if (error) {
      console.error('Supabase filter error:', error);
      return NextResponse.json(
        { success: false, error: 'Failed to fetch filtered data' },
        { status: 500 }
      );
    }

    const transformedData = data?.map(record => transformCallRecord(record)) || [];
    
    let analytics = null;
    if (includeAnalytics && transformedData.length > 0) {
      analytics = await processComprehensiveAnalytics(transformedData);
    }

    return NextResponse.json({
      success: true,
      data: transformedData,
      count: transformedData.length,
      analytics: analytics,
      filtersApplied: filters,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('Error in filtered fetch:', error);
    return NextResponse.json(
      { success: false, error: 'Internal server error' },
      { status: 500 }
    );
  }
}
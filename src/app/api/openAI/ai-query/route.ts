/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { NextRequest, NextResponse } from 'next/server';
import OpenAI from "openai";

// Initialize OpenAI client
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

interface CallOverviewData {
  contact_id: string;
  agent_username: string;
  initiation_timestamp: string;
  queue_name?: string;
  disposition_title?: string;
  campaign_name?: string;
  customer_cli?: string;
  call_duration: {
    minutes: number;
    seconds: number;
  };
  call_duration_total_seconds: number;
  call_summary?: string;
  primary_category?: string;
  categories?: string;
  sentiment_analysis?: string;
  processed_sentiments?: any[];
  created_at: string;
  updated_at: string;
  transcript_text?: string;
  transcript_processed?: {
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
  };
  keywords?: string;
  topics?: string;
  satisfaction_score?: number;
  resolution_status?: string;
  escalation_reason?: string;
  callback_requested?: boolean;
  language?: string;
  channel?: string;
  has_transcript: boolean;
  has_sentiment: boolean;
  has_summary: boolean;
  is_short_call: boolean;
  is_long_call: boolean;
  [key: string]: any;
}

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
}

interface Analytics {
  totalCalls: number;
  dateRange: {
    earliest: string;
    latest: string;
    spanDays: number;
  } | null;
  coverage: {
    transcript: { count: number; percentage: number };
    sentiment: { count: number; percentage: number };
    summary: { count: number; percentage: number };
  };
  agentAnalytics: Record<string, any>;
  sentimentAnalytics: {
    overall: { positive: number; neutral: number; negative: number };
    byAgent: Record<string, { positive: number; neutral: number; negative: number }>;
    byCategory: Record<string, { positive: number; neutral: number; negative: number }>;
    byQueue: Record<string, { positive: number; neutral: number; negative: number }>;
  };
  categoryAnalytics: {
    distribution: Record<string, number>;
    avgDurationByCategory: Record<string, number>;
    satisfactionByCategory: Record<string, number[]>;
  };
  dispositionAnalytics: {
    distribution: Record<string, number>;
    byAgent: Record<string, Record<string, number>>;
    byQueue: Record<string, Record<string, number>>;
    outcomeAnalysis: {
      sales: number;
      noSales: number;
      transfers: number;
      other: number;
    };
  };
  queueAnalytics: {
    distribution: Record<string, number>;
    avgDurationByQueue: Record<string, number>;
    satisfactionByQueue: Record<string, number[]>;
  };
  timeAnalytics: {
    hourlyVolume: Record<number, number>;
    dailyVolume: Record<string, number>;
    weeklyPatterns: Record<string, number>;
  };
}

export async function POST(request: NextRequest) {
  try {
    const { message, transcriptionData, conversationHistory, dataFields } = await request.json();

    if (!message) {
      return NextResponse.json(
        { error: "Message is required" },
        { status: 400 }
      );
    }

    if (!transcriptionData || !Array.isArray(transcriptionData)) {
      return NextResponse.json(
        { error: "Transcription data is required" },
        { status: 400 }
      );
    }

    // Perform basic data analytics
    const analytics = performBasicAnalytics(transcriptionData);
    
    // Build conversation context
    const conversationContext = buildConversationContext(conversationHistory);
    
    // Create system prompt with data overview
    const systemPrompt = createSystemPrompt(transcriptionData, analytics, dataFields, conversationContext);

    // Call OpenAI
    const completion = await openai.chat.completions.create({
      model: "gpt-4o",
      messages: [
        {
          role: "system",
          content: systemPrompt,
        },
        {
          role: "user",
          content: message,
        },
      ],
      max_tokens: 4000,
      temperature: 0.1,
      top_p: 0.9,
      frequency_penalty: 0.1,
      presence_penalty: 0.1,
    });

    const response = completion.choices[0]?.message?.content;

    if (!response) {
      throw new Error("No response from OpenAI");
    }

    return NextResponse.json({ response });

  } catch (error) {
    console.error("Error in AI query:", error);
    
    if (error instanceof Error && error.message.includes("API key")) {
      return NextResponse.json(
        { error: "OpenAI API key not configured" },
        { status: 500 }
      );
    }

    return NextResponse.json(
      { error: "Failed to process AI query" },
      { status: 500 }
    );
  }
}

function performBasicAnalytics(data: CallOverviewData[]): Analytics {
  const analytics: Analytics = {
    totalCalls: data.length,
    dateRange: getDateRange(data),
    coverage: calculateCoverage(data),
    agentAnalytics: processAgentAnalytics(data),
    sentimentAnalytics: processSentimentAnalytics(data),
    categoryAnalytics: processCategoryAnalytics(data),
    dispositionAnalytics: processDispositionAnalytics(data),
    queueAnalytics: processQueueAnalytics(data),
    timeAnalytics: processTimeAnalytics(data)
  };

  return analytics;
}

function getDateRange(data: CallOverviewData[]): {
  earliest: string;
  latest: string;
  spanDays: number;
} | null {
  if (data.length === 0) return null;
  
  const dates = data.map(call => new Date(call.initiation_timestamp).getTime());
  return {
    earliest: new Date(Math.min(...dates)).toISOString(),
    latest: new Date(Math.max(...dates)).toISOString(),
    spanDays: Math.ceil((Math.max(...dates) - Math.min(...dates)) / (1000 * 60 * 60 * 24))
  };
}

function calculateCoverage(data: CallOverviewData[]): {
  transcript: { count: number; percentage: number };
  sentiment: { count: number; percentage: number };
  summary: { count: number; percentage: number };
} {
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

function processAgentAnalytics(data: CallOverviewData[]) {
  const agentStats = {} as Record<string, any>;
  
  data.forEach(call => {
    const agent = call.agent_username;
    if (!agent) return;
    
    if (!agentStats[agent]) {
      agentStats[agent] = {
        totalCalls: 0,
        totalDuration: 0,
        avgDuration: 0,
        shortCalls: 0,
        longCalls: 0,
        sentimentCounts: { positive: 0, neutral: 0, negative: 0 },
        satisfactionScores: [] as number[],
        avgSatisfactionScore: 0,
        dispositions: {} as Record<string, number>,
        categories: {} as Record<string, number>,
        queues: {} as Record<string, number>
      };
    }
    
    const stats = agentStats[agent];
    stats.totalCalls++;
    stats.totalDuration += call.call_duration_total_seconds || 0;
    
    if (call.is_short_call) stats.shortCalls++;
    if (call.is_long_call) stats.longCalls++;
    
    // Track dispositions
    if (call.disposition_title) {
      stats.dispositions[call.disposition_title] = (stats.dispositions[call.disposition_title] || 0) + 1;
    }
    
    // Track categories
    if (call.primary_category) {
      stats.categories[call.primary_category] = (stats.categories[call.primary_category] || 0) + 1;
    }
    
    // Track queues
    if (call.queue_name) {
      stats.queues[call.queue_name] = (stats.queues[call.queue_name] || 0) + 1;
    }
    
    // Track satisfaction scores
    if (call.satisfaction_score !== null && call.satisfaction_score !== undefined) {
      stats.satisfactionScores.push(call.satisfaction_score);
    }
    
    // Track sentiment
    if (call.processed_sentiments && Array.isArray(call.processed_sentiments)) {
      call.processed_sentiments.forEach((item: any) => {
        const sentiment = item.sentiment?.toLowerCase();
        if (sentiment === 'positive') stats.sentimentCounts.positive++;
        else if (sentiment === 'negative') stats.sentimentCounts.negative++;
        else stats.sentimentCounts.neutral++;
      });
    }
  });
  
  // Calculate derived metrics
  Object.keys(agentStats).forEach(agent => {
    const stats = agentStats[agent];
    stats.avgDuration = stats.totalCalls > 0 ? stats.totalDuration / stats.totalCalls / 60 : 0;
    stats.avgSatisfactionScore = stats.satisfactionScores.length > 0 ? 
      stats.satisfactionScores.reduce((sum: number, score: number) => sum + score, 0) / stats.satisfactionScores.length : 0;
    
    const totalSentiments = stats.sentimentCounts.positive + stats.sentimentCounts.neutral + stats.sentimentCounts.negative;
    stats.positivePercentage = totalSentiments > 0 ? (stats.sentimentCounts.positive / totalSentiments) * 100 : 0;
    
    stats.topDisposition = Object.entries(stats.dispositions as Record<string, number>)
      .sort(([,a], [,b]) => b - a)[0]?.[0] || 'None';
    stats.topCategory = Object.entries(stats.categories as Record<string, number>)
      .sort(([,a], [,b]) => b - a)[0]?.[0] || 'None';
    stats.topQueue = Object.entries(stats.queues as Record<string, number>)
      .sort(([,a], [,b]) => b - a)[0]?.[0] || 'None';
  });
  
  return agentStats;
}

function processSentimentAnalytics(data: CallOverviewData[]): {
  overall: { positive: number; neutral: number; negative: number };
  byAgent: Record<string, { positive: number; neutral: number; negative: number }>;
  byCategory: Record<string, { positive: number; neutral: number; negative: number }>;
  byQueue: Record<string, { positive: number; neutral: number; negative: number }>;
} {
  const analytics = {
    overall: { positive: 0, neutral: 0, negative: 0 },
    byAgent: {} as Record<string, { positive: number, neutral: number, negative: number }>,
    byCategory: {} as Record<string, { positive: number, neutral: number, negative: number }>,
    byQueue: {} as Record<string, { positive: number, neutral: number, negative: number }>
  };
  
  data.forEach(call => {
    const agent = call.agent_username || 'Unknown';
    const category = call.primary_category || 'Uncategorized';
    const queue = call.queue_name || 'Unknown';
    
    if (!analytics.byAgent[agent]) {
      analytics.byAgent[agent] = { positive: 0, neutral: 0, negative: 0 };
    }
    if (!analytics.byCategory[category]) {
      analytics.byCategory[category] = { positive: 0, neutral: 0, negative: 0 };
    }
    if (!analytics.byQueue[queue]) {
      analytics.byQueue[queue] = { positive: 0, neutral: 0, negative: 0 };
    }
    
    if (call.processed_sentiments && Array.isArray(call.processed_sentiments)) {
      call.processed_sentiments.forEach((item: any) => {
        const sentiment = item.sentiment?.toLowerCase();
        if (sentiment === 'positive') {
          analytics.overall.positive++;
          analytics.byAgent[agent].positive++;
          analytics.byCategory[category].positive++;
          analytics.byQueue[queue].positive++;
        } else if (sentiment === 'negative') {
          analytics.overall.negative++;
          analytics.byAgent[agent].negative++;
          analytics.byCategory[category].negative++;
          analytics.byQueue[queue].negative++;
        } else {
          analytics.overall.neutral++;
          analytics.byAgent[agent].neutral++;
          analytics.byCategory[category].neutral++;
          analytics.byQueue[queue].neutral++;
        }
      });
    }
  });
  
  return analytics;
}

function processCategoryAnalytics(data: CallOverviewData[]): {
  distribution: Record<string, number>;
  avgDurationByCategory: Record<string, number>;
  satisfactionByCategory: Record<string, number[]>;
} {
  const analytics = {
    distribution: {} as Record<string, number>,
    avgDurationByCategory: {} as Record<string, number>,
    satisfactionByCategory: {} as Record<string, number[]>
  };
  
  data.forEach(call => {
    const category = call.primary_category || 'Uncategorized';
    
    analytics.distribution[category] = (analytics.distribution[category] || 0) + 1;
    
    if (!analytics.avgDurationByCategory[category]) {
      analytics.avgDurationByCategory[category] = 0;
    }
    analytics.avgDurationByCategory[category] += call.call_duration_total_seconds || 0;
    
    if (call.satisfaction_score !== null && call.satisfaction_score !== undefined) {
      if (!analytics.satisfactionByCategory[category]) {
        analytics.satisfactionByCategory[category] = [];
      }
      analytics.satisfactionByCategory[category].push(call.satisfaction_score);
    }
  });
  
  // Calculate averages
  Object.keys(analytics.avgDurationByCategory).forEach(category => {
    const count = analytics.distribution[category];
    analytics.avgDurationByCategory[category] = count > 0 ? 
      analytics.avgDurationByCategory[category] / count / 60 : 0; // Convert to minutes
  });
  
  return analytics;
}

function processDispositionAnalytics(data: CallOverviewData[]): {
  distribution: Record<string, number>;
  byAgent: Record<string, Record<string, number>>;
  byQueue: Record<string, Record<string, number>>;
  outcomeAnalysis: {
    sales: number;
    noSales: number;
    transfers: number;
    other: number;
  };
} {
  const analytics = {
    distribution: {} as Record<string, number>,
    byAgent: {} as Record<string, Record<string, number>>,
    byQueue: {} as Record<string, Record<string, number>>,
    outcomeAnalysis: {
      sales: 0,
      noSales: 0,
      transfers: 0,
      other: 0
    }
  };
  
  data.forEach(call => {
    if (!call.disposition_title) return;
    
    const disposition = call.disposition_title;
    const agent = call.agent_username || 'Unknown';
    const queue = call.queue_name || 'Unknown';
    
    analytics.distribution[disposition] = (analytics.distribution[disposition] || 0) + 1;
    
    if (!analytics.byAgent[agent]) {
      analytics.byAgent[agent] = {};
    }
    analytics.byAgent[agent][disposition] = (analytics.byAgent[agent][disposition] || 0) + 1;
    
    if (!analytics.byQueue[queue]) {
      analytics.byQueue[queue] = {};
    }
    analytics.byQueue[queue][disposition] = (analytics.byQueue[queue][disposition] || 0) + 1;
    
    // Categorize outcomes
    const dispositionLower = disposition.toLowerCase();
    if (dispositionLower.startsWith('sale')) {
      analytics.outcomeAnalysis.sales++;
    } else if (dispositionLower.startsWith('no sale')) {
      analytics.outcomeAnalysis.noSales++;
    } else if (dispositionLower.startsWith('transfer')) {
      analytics.outcomeAnalysis.transfers++;
    } else {
      analytics.outcomeAnalysis.other++;
    }
  });
  
  return analytics;
}

function processQueueAnalytics(data: CallOverviewData[]): {
  distribution: Record<string, number>;
  avgDurationByQueue: Record<string, number>;
  satisfactionByQueue: Record<string, number[]>;
} {
  const analytics = {
    distribution: {} as Record<string, number>,
    avgDurationByQueue: {} as Record<string, number>,
    satisfactionByQueue: {} as Record<string, number[]>
  };
  
  data.forEach(call => {
    const queue = call.queue_name || 'Unknown';
    
    analytics.distribution[queue] = (analytics.distribution[queue] || 0) + 1;
    
    if (!analytics.avgDurationByQueue[queue]) {
      analytics.avgDurationByQueue[queue] = 0;
    }
    analytics.avgDurationByQueue[queue] += call.call_duration_total_seconds || 0;
    
    if (call.satisfaction_score !== null && call.satisfaction_score !== undefined) {
      if (!analytics.satisfactionByQueue[queue]) {
        analytics.satisfactionByQueue[queue] = [];
      }
      analytics.satisfactionByQueue[queue].push(call.satisfaction_score);
    }
  });
  
  // Calculate averages
  Object.keys(analytics.avgDurationByQueue).forEach(queue => {
    const count = analytics.distribution[queue];
    analytics.avgDurationByQueue[queue] = count > 0 ? 
      analytics.avgDurationByQueue[queue] / count / 60 : 0; // Convert to minutes
  });
  
  return analytics;
}

function processTimeAnalytics(data: CallOverviewData[]): {
  hourlyVolume: Record<number, number>;
  dailyVolume: Record<string, number>;
  weeklyPatterns: Record<string, number>;
} {
  const analytics = {
    hourlyVolume: {} as Record<number, number>,
    dailyVolume: {} as Record<string, number>,
    weeklyPatterns: {} as Record<string, number>
  };
  
  data.forEach(call => {
    const date = new Date(call.initiation_timestamp);
    const hour = date.getHours();
    const dayKey = date.toISOString().split('T')[0];
    const dayOfWeek = date.toLocaleDateString('en-US', { weekday: 'long' });
    
    analytics.hourlyVolume[hour] = (analytics.hourlyVolume[hour] || 0) + 1;
    analytics.dailyVolume[dayKey] = (analytics.dailyVolume[dayKey] || 0) + 1;
    analytics.weeklyPatterns[dayOfWeek] = (analytics.weeklyPatterns[dayOfWeek] || 0) + 1;
  });
  
  return analytics;
}

function createSystemPrompt(
  data: CallOverviewData[], 
  analytics: Analytics, 
  dataFields: string[], 
  conversationContext: string
): string {
  const topAgents = Object.entries(analytics.agentAnalytics)
    .sort(([,a], [,b]) => (b as any).positivePercentage - (a as any).positivePercentage)
    .slice(0, 10);
  
  const topCategories = Object.entries(analytics.categoryAnalytics.distribution as Record<string, number>)
    .sort(([,a], [,b]) => (b as number) - (a as number))
    .slice(0, 5);

  const topDispositions = Object.entries(analytics.dispositionAnalytics.distribution as Record<string, number>)
    .sort(([,a], [,b]) => (b as number) - (a as number))
    .slice(0, 10);

  const topQueues = Object.entries(analytics.queueAnalytics.distribution as Record<string, number>)
    .sort(([,a], [,b]) => (b as number) - (a as number))
    .slice(0, 5);

  const sentimentOverview = analytics.sentimentAnalytics.overall;
  const totalSentiments = sentimentOverview.positive + sentimentOverview.neutral + sentimentOverview.negative;
  const positivityRate = totalSentiments > 0 ? ((sentimentOverview.positive / totalSentiments) * 100).toFixed(1) : '0';

  return `You are TSA's Call Analytics AI with access to comprehensive call center data. You have ${analytics.totalCalls} actual call records spanning ${analytics.dateRange?.spanDays || 0} days.

=== DATASET OVERVIEW ===

📊 CALL CENTER DATA ACCESS:
- Total Calls: ${analytics.totalCalls}
- Call Date Range: ${analytics.dateRange?.earliest.split('T')[0]} to ${analytics.dateRange?.latest.split('T')[0]}
- Transcript Coverage: ${analytics.coverage.transcript.count}/${analytics.totalCalls} calls (${analytics.coverage.transcript.percentage.toFixed(1)}%)
- Sentiment Coverage: ${analytics.coverage.sentiment.count}/${analytics.totalCalls} calls (${analytics.coverage.sentiment.percentage.toFixed(1)}%)
- Overall Positivity Rate: ${positivityRate}%

👥 TOP AGENT PERFORMANCE:
${topAgents.map(([agent, stats]: [string, any]) => 
  `• ${agent}: ${stats.totalCalls} calls, ${stats.positivePercentage.toFixed(1)}% positive sentiment, ${stats.avgDuration.toFixed(1)} min avg duration, Top: ${stats.topDisposition}`
).join('\n')}

📋 CALL CATEGORIES (Top 5):
${topCategories.map(([category, count]) => 
  `• ${category}: ${count} calls (${((count / analytics.totalCalls) * 100).toFixed(1)}%)`
).join('\n')}

📞 QUEUE DISTRIBUTION (Top 5):
${topQueues.map(([queue, count]) => 
  `• ${queue}: ${count} calls (${((count / analytics.totalCalls) * 100).toFixed(1)}%)`
).join('\n')}

🎯 CALL DISPOSITIONS (Top 10):
${topDispositions.map(([disposition, count]) => 
  `• ${disposition}: ${count} calls (${((count / analytics.totalCalls) * 100).toFixed(1)}%)`
).join('\n')}

💰 OUTCOME ANALYSIS:
- Sales: ${analytics.dispositionAnalytics.outcomeAnalysis.sales} calls
- No Sales: ${analytics.dispositionAnalytics.outcomeAnalysis.noSales} calls  
- Transfers: ${analytics.dispositionAnalytics.outcomeAnalysis.transfers} calls
- Other: ${analytics.dispositionAnalytics.outcomeAnalysis.other} calls

📊 SENTIMENT BREAKDOWN:
- Positive: ${sentimentOverview.positive} sentiments
- Neutral: ${sentimentOverview.neutral} sentiments
- Negative: ${sentimentOverview.negative} sentiments

=== AVAILABLE DATA FIELDS ===
${dataFields?.join(', ') || 'Standard call center fields available'}

=== RESPONSE GUIDELINES ===
✅ Use actual numbers and statistics from the real data provided
✅ Reference specific agents, categories, dispositions, and queues from the dataset
✅ Provide data-driven insights and analysis
✅ Answer questions about patterns, trends, and performance metrics
✅ Include relevant context like call volumes, time periods, and percentages
✅ When discussing transcripts, reference actual transcript content when available
✅ Break down complex questions into multiple insights when helpful

❌ Never create hypothetical data or examples
❌ Never reference non-existent agents, categories, or metrics
❌ Never provide made-up statistics or percentages
❌ Don't speculate beyond what the data shows

${conversationContext ? `\n📝 CONVERSATION CONTEXT:\n${conversationContext}` : ''}

Remember: Every statistic, comparison, and insight must be derived from the actual call data provided. Focus on helping users understand their call center performance through data-driven analysis.`;
}

function buildConversationContext(conversationHistory: Message[]): string {
  if (!conversationHistory || conversationHistory.length === 0) return '';
  
  const recentMessages = conversationHistory.slice(-4);
  const context = recentMessages
    .map(msg => `${msg.role.toUpperCase()}: ${msg.content}`)
    .join('\n');
  
  return `Recent Conversation:\n${context}`;
}
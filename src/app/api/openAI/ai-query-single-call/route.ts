import { NextRequest, NextResponse } from "next/server";
import OpenAI from "openai";

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

interface CallTranscriptionData {
  contact_id: string;
  recording_location: string;
  transcript_text: string;
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
    minutes: number | 0;
    seconds: number | 0;
  };
  time_in_queue?: number;
  created_at?: string;
  updated_at?: string;
  call_duration: {
    minutes: number | 0;
    seconds: number | 0;
  };
}

interface Message {
  role: "user" | "assistant";
  content: string;
}

export async function POST(req: NextRequest) {
  try {
    const { message, callData, conversationHistory } = await req.json();

    if (!message) {
      return NextResponse.json(
        { error: "Message is required" },
        { status: 400 }
      );
    }

    if (!callData) {
      return NextResponse.json(
        { error: "Call data is required" },
        { status: 400 }
      );
    }

    // Parse JSON fields for better analysis
    const parsedSpeakerData = parseJsonField(callData.speaker_data);
    const parsedSentimentData = parseJsonField(callData.sentiment_analysis);
    const parsedEntities = parseJsonField(callData.entities);
    const parsedCategories = parseJsonField(callData.categories);

    // Generate comprehensive call context
    const callContext = generateCallContext(callData, {
      speakerData: parsedSpeakerData,
      sentimentData: parsedSentimentData,
      entities: parsedEntities,
      categories: parsedCategories,
    });

    const systemPrompt = `You are a helpful assistant that analyzes call transcripts and provides insights. 
You have access to a call transcript and analysis data. 
Use this information to answer user questions accurately and concisely.
Only use information that's available in the provided transcript and analysis data.
If you don't know the answer, say so honestly.

FORMATTING GUIDELINES:
- Format your responses using proper markdown for readability
- Use headers (##, ###) to organize information
- Use bullet points for lists and key points
- Use **bold** for emphasis on important information
- Use backticks for specific terms or quotes from the transcript
- Use blockquotes (>) for direct quotes from the call
- Keep responses well-structured and scannable
- Include specific examples and quotes when relevant

CALL CONTEXT:
${callContext}

Key capabilities:
- Analyze the conversation flow and interaction quality
- Identify key moments, issues, and resolutions
- Assess agent performance and customer satisfaction
- Extract insights from sentiment patterns
- Explain technical details about the call
- Provide specific quotes and examples from the transcript
- Answer questions about any aspect of this call

When answering:
- Be specific and reference actual content from the call
- Use quotes from the transcript when relevant
- Reference specific speakers (Agent/Customer) and timeframes when possible
- Provide context and explain your analysis clearly
- If asked about sentiment, reference specific segments with examples
- For performance questions, analyze both agent and customer behavior
- Keep responses focused on this specific call
- Format responses clearly using markdown for better readability

The user can ask about any aspect of this call data.`;

    // Prepare the conversation messages
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const messages: any[] = [{ role: "system", content: systemPrompt }];

    // Add conversation history (last few messages for context)
    if (conversationHistory && Array.isArray(conversationHistory)) {
      conversationHistory.slice(-8).forEach((msg: Message) => {
        messages.push({
          role: msg.role,
          content: msg.content,
        });
      });
    }

    // Add the current user message
    messages.push({
      role: "user",
      content: message,
    });

    const completion = await openai.chat.completions.create({
      model: "gpt-4o",
      messages: messages,
      max_tokens: 1500, // Increased for better formatted responses
      temperature: 0.3, // Lower temperature for more focused analysis
    });

    const response =
      completion.choices[0]?.message?.content ||
      "Sorry, I could not generate a response.";

    return NextResponse.json({ response });
  } catch (error) {
    console.error("Error in single call AI query:", error);
    return NextResponse.json(
      { error: "Failed to process AI query. Please try again." },
      { status: 500 }
    );
  }
}

function parseJsonField(jsonString: string | null | undefined) {
  if (!jsonString) return null;
  try {
    return JSON.parse(jsonString);
  } catch {
    return null;
  }
}

function generateCallContext(
  callData: CallTranscriptionData,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  parsedData: any
): string {
  const { speakerData, sentimentData, entities, categories } = parsedData;

  // Format call duration
  const formatDuration = (duration: {
    minutes: number | 0;
    seconds: number | 0;
  }) => {
    return `${duration.minutes || 0}m ${duration.seconds || 0}s`;
  };

  // Generate sentiment summary with better formatting
  const sentimentSummary = sentimentData
    ? generateSentimentSummary(sentimentData)
    : "No sentiment analysis available";

  // Generate entity summary with better formatting
  const entitySummary =
    entities && entities.length > 0
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ? entities.map((e: any) => `- **${e.entity_type}**: "${e.text}" (confidence: ${Math.round(e.confidence * 100)}%)`).join("\n")
      : "No entities extracted";

  // Generate category summary with better formatting
  const categorySummary =
    categories && categories.length > 0
      ? categories
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          .map((c: any) => `- ${c.category} (${Math.round(c.confidence * 100)}% confidence)`)
          .join("\n")
      : "No categories identified";

  // Format transcript for context with better structure
  const transcriptPreview = generateTranscriptPreview(
    speakerData,
    callData.transcript_text
  );

  return `
## CALL METADATA
- **Call ID**: ${callData.contact_id}
- **Date/Time**: ${new Date(callData.initiation_timestamp).toLocaleString()}
- **Agent**: ${callData.agent_username}
- **Queue**: ${callData.queue_name || "N/A"}
- **Campaign**: ${callData.campaign_name || "N/A"}
- **Customer CLI**: ${callData.customer_cli || "N/A"}
- **Duration**: ${formatDuration(callData.call_duration)}
- **Hold Time**: ${formatDuration(
    callData.total_hold_time || { minutes: 0, seconds: 0 }
  )}
- **Queue Time**: ${callData.time_in_queue || 0} seconds
- **Disposition**: ${callData.disposition_title || "N/A"}

## CALL SUMMARY
${callData.call_summary || "No AI summary available"}

## SENTIMENT ANALYSIS
${sentimentSummary}

## EXTRACTED ENTITIES
${entitySummary}

## CALL CATEGORIES
${categorySummary}

## TRANSCRIPT PREVIEW
${transcriptPreview}

## FULL TRANSCRIPT
${callData.transcript_text || "No transcript available"}
`;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function generateSentimentSummary(sentimentData: any[]): string {
  if (!sentimentData || sentimentData.length === 0)
    return "No sentiment data available";

  const sentimentCounts = sentimentData.reduce((counts, item) => {
    const sentiment = item.sentiment.toLowerCase();
    counts[sentiment] = (counts[sentiment] || 0) + 1;
    return counts;
  }, {} as Record<string, number>);

  const total = sentimentData.length;

  const summaryItems = Object.entries(sentimentCounts)
    .map(([sentiment, count]) => {
      const numCount = count as number;
      const percentage = Math.round((numCount / total) * 100);
      const emoji = sentiment === 'positive' ? '😊' : sentiment === 'negative' ? '😞' : '😐';
      return `- **${sentiment.charAt(0).toUpperCase() + sentiment.slice(1)}** ${emoji}: ${numCount}/${total} (${percentage}%)`;
    })
    .join("\n");

  // Add some example sentiment segments
  const exampleSentiments = sentimentData
    .slice(0, 3)
    .map(item => `> "${item.text}" - *${item.sentiment}* (${Math.round(item.confidence * 100)}% confidence)`)
    .join("\n");

  return `**Overall sentiment distribution:**
${summaryItems}

**Sample sentiment analysis:**
${exampleSentiments}`;
}

function generateTranscriptPreview(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  speakerData: any[],
  fallbackText: string
): string {
  if (speakerData && speakerData.length > 0) {
    // Show first few exchanges to give context with better formatting
    const preview = speakerData
      .slice(0, 6)
      .map(
        (utterance) =>
          `**${utterance.speaker}** (${Math.floor(utterance.start / 1000)}s): "${utterance.text.substring(0, 150)}${
            utterance.text.length > 150 ? "..." : ""
          }"`
      )
      .join("\n\n");

    return `${preview}${
      speakerData.length > 6
        ? `\n\n*[... ${speakerData.length - 6} more exchanges ...]*`
        : ""
    }`;
  } else {
    // Fallback to plain text preview
    const preview = fallbackText
      ? fallbackText.substring(0, 500)
      : "No transcript available";
    return `${preview}${
      fallbackText && fallbackText.length > 500 ? "..." : ""
    }`;
  }
}
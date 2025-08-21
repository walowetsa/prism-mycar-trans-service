/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-explicit-any */
// app/api/transcribe/status/[id]/route.ts
import { NextResponse } from "next/server";

/**
 * Helper function to save transcription to Supabase
 */
async function saveToSupabase(
  callData: any,
  transcriptData: any,
  categorization: any = null
) {
  try {
    const serverUrl = process.env.NEXT_PUBLIC_SERVER_URL || "http://192.168.40.101";

    const payload = {
      contact_id: callData.contact_id,
      recording_location: callData.recording_location || "",
      transcript_text: transcriptData.text || "",
      queue_name: callData.queue_name || null,
      agent_username: callData.agent_username || "",
      initiation_timestamp: callData.initiation_timestamp || new Date().toISOString(),
      speaker_data: transcriptData.utterances ? JSON.stringify(transcriptData.utterances) : null,
      sentiment_analysis: transcriptData.sentiment_analysis_results ? JSON.stringify(transcriptData.sentiment_analysis_results) : null,
      entities: transcriptData.entities ? JSON.stringify(transcriptData.entities) : null,
      disposition_title: callData.disposition_title || null,
      call_summary: transcriptData.summary || null,
      campaign_name: callData.campaign_name || null,
      campaign_id: callData.campaign_id || null,
      customer_cli: callData.customer_cli || null,
      agent_hold_time: callData.agent_hold_time || null,
      total_hold_time: callData.total_hold_time || null,
      time_in_queue: callData.time_in_queue || null,
      call_duration: callData.total_call_time || null,
      categories: categorization?.topic_categories 
        ? JSON.stringify(categorization.topic_categories) 
        : (transcriptData.topic_categorization?.all_topics ? JSON.stringify(transcriptData.topic_categorization.all_topics) : null),
      primary_category: categorization?.primary_category || transcriptData.topic_categorization?.primary_topic || null,
    };

    const response = await fetch(`${serverUrl}/api/supabase/save-transcription`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Supabase save failed: ${response.status}`);
    }

    const result = await response.json();
    return result.data;
  } catch (error) {
    console.error("Error saving to Supabase:", error);
    throw error;
  }
}

/**
 * Get server URL with proper network configuration
 */
function getServerUrl(): string {
  const possibleUrls = [
    process.env.NEXT_PUBLIC_SERVER_URL,
    process.env.NETWORK_URL,
    'http://192.168.40.101'
  ].filter(Boolean) as string[];
  
  return possibleUrls[0] || 'http://192.168.40.101';
}

/**
 * Topic categorization with timeout
 */
async function performTopicCategorization(transcriptData: any): Promise<{
  primary_category: string;
  topic_categories: string[];
  confidence: number;
} | null> {
  try {
    const serverUrl = getServerUrl();

    const response = await fetch(`${serverUrl}/api/openAI/categorise`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ transcript: transcriptData }),
      signal: AbortSignal.timeout(20000), // 20 seconds for categorization
    });

    if (!response.ok) {
      console.error("Topic categorization failed:", response.status);
      return null;
    }

    const topicData = await response.json();

    if (topicData.topic_categories && topicData.topic_categories.length > 0) {
      return {
        primary_category: topicData.primary_category,
        topic_categories: topicData.topic_categories,
        confidence: topicData.confidence || 1.0,
      };
    }

    return null;
  } catch (error) {
    console.error("Error in topic categorization:", error);
    return null;
  }
}

export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params; // FIXED: Await params for Next.js 15
    const url = new URL(request.url);
    const callDataParam = url.searchParams.get('callData');
    
    let callData = null;
    if (callDataParam) {
      try {
        callData = JSON.parse(decodeURIComponent(callDataParam));
      } catch (e) {
        console.warn("Failed to parse callData parameter");
      }
    }

    console.log(`📊 Checking transcription status for job: ${id}`);

    const apiKey = process.env.ASSEMBLYAI_API_KEY;
    if (!apiKey) {
      return NextResponse.json({ error: "AssemblyAI API key not configured" }, { status: 500 });
    }

    // Get transcription status from AssemblyAI
    const statusResponse = await fetch(`https://api.assemblyai.com/v2/transcript/${id}`, {
      headers: { Authorization: apiKey },
      signal: AbortSignal.timeout(15000), // 15 second timeout for status check
    });

    if (!statusResponse.ok) {
      console.error("AssemblyAI status check failed:", statusResponse.status);
      return NextResponse.json({
        error: "Failed to check transcription status",
        status: "error"
      }, { status: 500 });
    }

    const transcript = await statusResponse.json();
    const status = transcript.status;

    console.log(`📋 Job ${id} status: ${status}`);

    if (status === "completed") {
      console.log("✅ Transcription completed! Processing results...");

      // Process speaker roles
      if (transcript.utterances) {
        transcript.utterances = transcript.utterances.map((utterance: any) => ({
          ...utterance,
          speakerRole: utterance.speaker === "A" ? "Agent" : "Customer",
        }));
      }

      if (transcript.words) {
        transcript.words = transcript.words.map((word: any) => ({
          ...word,
          speakerRole: word.speaker === "A" ? "Agent" : "Customer",
        }));
      }

      // Optional topic categorization
      let categorization: {
        primary_category: string;
        topic_categories: string[];
        confidence: number;
      } | null = null;
      
      if (transcript.utterances && transcript.utterances.length > 0) {
        try {
          categorization = await performTopicCategorization(transcript);
        } catch (catError) {
          console.error("⚠️ Categorization failed:", catError);
          categorization = null;
        }
        
        transcript.topic_categorization = categorization ? {
          primary_topic: categorization.primary_category,
          all_topics: categorization.topic_categories,
          confidence: categorization.confidence,
        } : {
          primary_topic: "Uncategorised",
          all_topics: ["Uncategorised"],
          confidence: 0,
        };
      }

      // Optional Supabase save
      if (callData) {
        try {
          await saveToSupabase(callData, transcript, categorization);
          console.log("✅ Saved to Supabase");
        } catch (supabaseError) {
          console.error("⚠️ Supabase save failed:", supabaseError);
        }
      }

      console.log(`🎉 Transcription processing complete for job: ${id}`);

      return NextResponse.json({
        ...transcript,
        status: "completed",
        call_data: callData || null,
      });

    } else if (status === "error") {
      console.error("❌ AssemblyAI transcription error:", transcript.error);
      return NextResponse.json({
        error: `Transcription failed: ${transcript.error || "Unknown error"}`,
        status: "error",
        details: transcript.error
      }, { status: 500 });
      
    } else {
      // Still processing
      return NextResponse.json({
        status: status,
        transcription_id: id,
        message: `Transcription is ${status}`,
        call_data: callData || null,
      });
    }

  } catch (error) {
    console.error("💥 Status check failed:", error);
    return NextResponse.json({
      error: "Failed to check transcription status",
      message: error instanceof Error ? error.message : "Unknown Error",
      status: "error"
    }, { status: 500 });
  }
}
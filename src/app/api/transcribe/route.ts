/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-explicit-any */
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

// REMOVED: No longer needed with infrastructure bypass approach
// downloadAudioWithChunking and uploadToAssemblyAI functions removed
// since we now use direct URLs

/**
 * INFRASTRUCTURE BYPASS: Use direct SFTP URL approach
 */
async function getOptimizedAudioUrl(sftpFilename: string, apiKey: string): Promise<string> {
  console.log("🎯 INFRASTRUCTURE BYPASS: Using direct URL approach:", sftpFilename);
  
  const serverUrl = getServerUrl();
  const decodedFilename = decodeURIComponent(sftpFilename);
  const directUrl = `${serverUrl}/api/sftp/download?filename=${encodeURIComponent(decodedFilename)}`;
  
  // INFRASTRUCTURE BYPASS: Always use direct URL - let AssemblyAI handle the download
  console.log("🚀 INFRASTRUCTURE BYPASS: Using direct URL to avoid server timeout");
  console.log(`🎯 Direct URL: ${directUrl}`);
  
  // Quick validation that the file exists (but don't download it ourselves)
  try {
    console.log("📋 Quick file validation...");
    const headResponse = await fetch(directUrl, { 
      method: 'HEAD',
      signal: AbortSignal.timeout(15000) // Quick 15-second check only
    });
    
    if (headResponse.ok) {
      const contentLength = headResponse.headers.get('content-length');
      const sizeInMB = contentLength ? parseInt(contentLength) / (1024 * 1024) : 0;
      
      console.log(`✅ File validated! Size: ${sizeInMB.toFixed(2)}MB - letting AssemblyAI download directly`);
      return directUrl;
    } else {
      console.log(`⚠️ File validation failed (${headResponse.status}), but proceeding with direct URL anyway`);
      return directUrl;
    }
  } catch (validationError) {
    console.log(`⚠️ File validation error, but proceeding with direct URL: ${validationError instanceof Error ? validationError.message : 'Unknown'}`);
    return directUrl;
  }
}

/**
 * Topic categorization with infrastructure timeout protection
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
      signal: AbortSignal.timeout(20000), // 20 seconds to avoid infrastructure timeout
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

export async function POST(request: Request) {
  const requestStartTime = Date.now();
  
  try {
    const body = await request.json();
    const {
      audioUrl,
      speakerCount = 2,
      filename,
      isDirectSftpFile = false,
      sftpFilename = null,
      callData = null,
    } = body;

    console.log("🎬 INFRASTRUCTURE BYPASS TRANSCRIPTION:", {
      filename,
      isDirectSftpFile,
      sftpFilename,
      callId: callData?.contact_id
    });

    // Validation
    if (!audioUrl && !isDirectSftpFile) {
      return NextResponse.json({ error: "Audio source required" }, { status: 400 });
    }

    if (!filename) {
      return NextResponse.json({ error: "Filename required" }, { status: 400 });
    }

    const apiKey = process.env.ASSEMBLYAI_API_KEY;
    if (!apiKey) {
      return NextResponse.json({ error: "AssemblyAI API key not configured" }, { status: 500 });
    }

    let uploadUrl: string;

    // PHASE 1: Get audio URL (INFRASTRUCTURE BYPASS)
    try {
      console.log("📁 PHASE 1: Audio URL resolution (INFRASTRUCTURE BYPASS)...");
      
      if (isDirectSftpFile && sftpFilename) {
        uploadUrl = await getOptimizedAudioUrl(sftpFilename, apiKey);
      } else if (audioUrl) {
        console.log("Using provided audio URL:", audioUrl);
        uploadUrl = audioUrl;
      } else {
        throw new Error("No valid audio source");
      }

      console.log(`✅ Audio URL ready (INFRASTRUCTURE BYPASS): ${uploadUrl.substring(0, 100)}...`);

    } catch (audioError) {
      console.error("❌ Audio processing failed:", audioError);
      
      return NextResponse.json({
        error: "Failed to process audio file",
        details: audioError instanceof Error ? audioError.message : "Unknown error",
        stage: "audio_processing"
      }, { status: 500 });
    }

    // PHASE 2: Submit to AssemblyAI (Quick operation)
    console.log("📡 PHASE 2: Submitting to AssemblyAI (INFRASTRUCTURE BYPASS)...");
    
    const transcriptResponse = await fetch("https://api.assemblyai.com/v2/transcript", {
      method: "POST",
      headers: {
        Authorization: apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        audio_url: uploadUrl,
        speech_model: "best",
        speaker_labels: true,
        speakers_expected: speakerCount || 2,
        summarization: true,
        summary_model: "conversational",
        summary_type: "paragraph",
        entity_detection: true,
        sentiment_analysis: true,
        filter_profanity: false,
        auto_highlights: true,
        punctuate: true,
        format_text: true,
      }),
      signal: AbortSignal.timeout(30000), // 30 seconds should be plenty for submission
    });

    if (!transcriptResponse.ok) {
      const errorData = await transcriptResponse.json();
      console.error("AssemblyAI submission failed:", errorData);
      return NextResponse.json({
        error: "Failed to initiate transcription",
        details: errorData
      }, { status: 500 });
    }

    const { id } = await transcriptResponse.json();
    console.log(`✅ Transcription job created (INFRASTRUCTURE BYPASS): ${id}`);

    // PHASE 3: Quick initial poll, then return with job ID for client-side polling
    console.log("⏳ PHASE 3: Initial status check...");
    
    let transcript;
    let status = "processing";
    let attempts = 0;
    const maxQuickAttempts = 6; // Only 30 seconds of polling (6 * 5 seconds)
    const pollInterval = 5000;

    // Do a few quick polls to catch fast transcriptions
    while ((status === "processing" || status === "queued") && attempts < maxQuickAttempts) {
      await new Promise(resolve => setTimeout(resolve, pollInterval));
      attempts++;

      try {
        const statusResponse = await fetch(`https://api.assemblyai.com/v2/transcript/${id}`, {
          headers: { Authorization: apiKey },
          signal: AbortSignal.timeout(10000),
        });

        if (!statusResponse.ok) {
          console.error("Status check failed:", statusResponse.status);
          break;
        }

        transcript = await statusResponse.json();
        status = transcript.status;
        
        console.log(`📊 Quick poll ${attempts}/${maxQuickAttempts}: ${status}`);
        
        if (status === "completed" || status === "error") {
          break;
        }
        
      } catch (statusError) {
        console.error("Status check error:", statusError);
        break;
      }
    }

    // PHASE 4: Process results if completed quickly, otherwise return job ID
    if (status === "completed" && transcript) {
      console.log("✅ Transcription completed quickly! (INFRASTRUCTURE BYPASS)");

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

      // Optional topic categorization (with timeout to avoid infrastructure timeout)
      let categorization: {
        primary_category: string;
        topic_categories: string[];
        confidence: number;
      } | null = null;
      
      if (transcript.utterances && transcript.utterances.length > 0) {
        try {
          // Use AbortSignal.timeout for better type safety
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

      const totalTime = Date.now() - requestStartTime;
      console.log(`🎉 QUICK TRANSCRIPTION COMPLETE in ${Math.round(totalTime/1000)}s (INFRASTRUCTURE BYPASS)`);

      return NextResponse.json({
        ...transcript,
        status: "completed",
        call_data: callData || null,
      });

    } else if (status === "error") {
      console.error("❌ AssemblyAI error:", transcript?.error);
      return NextResponse.json({
        error: `Transcription failed: ${transcript?.error || "Unknown error"}`,
        status: "error",
      }, { status: 500 });
      
    } else {
      // Transcription is still processing - return job ID for client-side polling
      const totalTime = Date.now() - requestStartTime;
      console.log(`⏳ Transcription still processing after ${Math.round(totalTime/1000)}s - returning job ID for client polling (INFRASTRUCTURE BYPASS)`);
      
      return NextResponse.json({
        status: "processing",
        transcription_id: id,
        message: "Transcription started successfully. AssemblyAI is processing the file directly from your server.",
        polling_url: `/api/transcribe/status/${id}`,
        call_data: callData || null,
      });
    }

  } catch (error) {
    console.error("💥 TRANSCRIPTION FAILED (INFRASTRUCTURE BYPASS):", error);
    return NextResponse.json({
      error: "Internal server error",
      message: error instanceof Error ? error.message : "Unknown Error",
      status: "error"
    }, { status: 500 });
  }
}
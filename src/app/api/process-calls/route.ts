/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-explicit-any */

import { NextRequest, NextResponse } from "next/server";
import { Pool } from "pg";
import { createClient } from "@supabase/supabase-js";
import { Client } from "ssh2";
import { readFileSync } from "fs";
import * as path from "path";

// db
const pool = new Pool({
  host: process.env.DB_HOST,
  port: 5432,
  user: process.env.DB_USER,
  password: "gR!eKm^t|iM$2H0TW=3yN.MI,RT9E~.oQ.YUt!mKCT",
  database: process.env.DB_NAME,
});

// Supabase config
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

// SFTP config
type SftpConfig = {
  host: string;
  port: number;
  username: string;
  privateKey: Buffer;
  passphrase: string;
};

function getSftpConfig(): SftpConfig {
  return {
    host: process.env.SFTP_HOST!,
    port: parseInt(process.env.SFTP_PORT!),
    username: process.env.SFTP_USERNAME!,
    privateKey: readFileSync(
      path.resolve(process.env.HOME || "~", ".ssh/sftp_key")
    ),
    passphrase: process.env.SFTP_PASSPHRASE!,
  };
}

interface DateRange {
  start: Date;
  end: Date;
}

interface CallLog {
  contact_id: string;
  agent_username: string;
  recording_location: string;
  initiation_timestamp: string;
  total_call_time: {
    minutes: number;
    seconds: number;
  };
  campaign_name: string;
  campaign_id: number;
  customer_cli: string;
  agent_hold_time: number;
  total_hold_time: number;
  time_in_queue: number;
  queue_name: string;
  disposition_title: string;
  existsInSupabase?: boolean;
}

interface TranscriptionData {
  contact_id: string;
  recording_location: string;
  transcript_text: string;
  queue_name?: string;
  agent_username: string;
  initiation_timestamp: string;
  speaker_data?: string | null;
  sentiment_analysis?: string | null;
  entities?: string | null;
  categories?: string | null;
  disposition_title?: string;
  call_summary?: string | null;
  campaign_name?: string | null;
  campaign_id?: string | null;
  customer_cli?: string | null;
  agent_hold_time?: number | null;
  total_hold_time?: number | null;
  time_in_queue?: number | null;
  call_duration: string;
  primary_category?: string | null;
}

async function getContactLogs(dateRange?: DateRange) {
  try {
    let query: string;
    let params: any[] = [];

    if (dateRange) {
      query = `
        SELECT * FROM reporting.contact_log 
        WHERE agent_username IS NOT NULL
        AND disposition_title IS NOT NULL
        AND recording_location LIKE '%.mp3%' 
        AND initiation_timestamp >= $1 
        AND initiation_timestamp <= $2
        ORDER BY initiation_timestamp DESC
      `;
      params = [dateRange.start, dateRange.end];
    } else {
      query = `
        SELECT * FROM reporting.contact_log 
        WHERE agent_username IS NOT NULL
        AND disposition_title IS NOT NULL
        AND recording_location LIKE '%.mp3%' 
        ORDER BY initiation_timestamp DESC
      `;
    }

    const result = await pool.query(query, params);
    return result.rows;
  } catch (error) {
    console.error("Database query error:", error);
    throw error;
  }
}

async function checkCallExistsInSupabase(contactId: string): Promise<boolean> {
  try {
    const { data, error } = await supabase
      .from("call_records")
      .select("contact_id")
      .eq("contact_id", contactId)
      .single();

    if (error && error.code !== "PGRST116") {
      console.error(`Error checking call ${contactId}:`, error);
      return false;
    }

    return !!data;
  } catch (error) {
    console.error(`Error checking call ${contactId}:`, error);
    return false;
  }
}

async function enhanceCallLogsWithSupabaseStatus(
  logs: CallLog[]
): Promise<CallLog[]> {
  try {
    if (logs.length === 0) return logs;

    const contactIds = logs.map((log) => log.contact_id);
    console.log(
      `🔍 Checking ${contactIds.length} contact IDs against Supabase in batches...`
    );

    const SUPABASE_BATCH_SIZE = 100;
    const allExistingContactIds = new Set<string>();

    for (let i = 0; i < contactIds.length; i += SUPABASE_BATCH_SIZE) {
      const batch = contactIds.slice(i, i + SUPABASE_BATCH_SIZE);
      const batchNumber = Math.floor(i / SUPABASE_BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(contactIds.length / SUPABASE_BATCH_SIZE);

      console.log(
        `📊 Checking Supabase batch ${batchNumber}/${totalBatches} (${batch.length} contact IDs)`
      );

      try {
        const { data: batchRecords, error } = await supabase
          .from("call_records")
          .select("contact_id")
          .in("contact_id", batch);

        if (error) {
          console.error(`Error in Supabase batch ${batchNumber}:`, error);
          continue;
        }

        if (batchRecords) {
          batchRecords.forEach((record) =>
            allExistingContactIds.add(record.contact_id)
          );
        }

        console.log(
          `✅ Batch ${batchNumber}/${totalBatches}: Found ${
            batchRecords?.length || 0
          } existing records`
        );
      } catch (batchError) {
        console.error(
          `Error processing Supabase batch ${batchNumber}:`,
          batchError
        );
        continue;
      }
    }

    console.log(
      `📋 Total existing records found: ${allExistingContactIds.size}/${contactIds.length}`
    );

    return logs.map((log) => ({
      ...log,
      existsInSupabase: allExistingContactIds.has(log.contact_id),
    }));
  } catch (error) {
    console.error("Error enhancing call logs with Supabase status:", error);
    return logs.map((log) => ({ ...log, existsInSupabase: false }));
  }
}

function constructSftpPath(filename: string): string[] {
  const possiblePaths = [];

  let decodedFilename = filename;
  try {
    decodedFilename = decodeURIComponent(filename);
  } catch (error) {
    console.log(`⚠️ Could not decode filename: ${filename}`);
  }

  if (decodedFilename.includes("/")) {
    let cleanPath = decodedFilename;

    if (cleanPath.startsWith("amazon-connect-b1a9c08821e5/")) {
      cleanPath = cleanPath.replace("amazon-connect-b1a9c08821e5/", "");
    }

    if (!cleanPath.startsWith("./") && !cleanPath.startsWith("/")) {
      cleanPath = `./${cleanPath}`;
    }

    possiblePaths.push(cleanPath);

    if (cleanPath.startsWith("./")) {
      possiblePaths.push(cleanPath.substring(2));
    }
  }

  const justFilename = decodedFilename.split("/").pop() || decodedFilename;

  const currentDate = new Date();
  const currentYear = currentDate.getFullYear();

  for (let daysForward = 0; daysForward <= 1; daysForward++) {
    const targetDate = new Date(currentDate);
    targetDate.setDate(currentDate.getDate() + daysForward);

    const year = targetDate.getFullYear();
    const month = targetDate.getMonth() + 1;
    const day = targetDate.getDate();

    const datePath = `${year}/${month.toString().padStart(2, "0")}/${day
      .toString()
      .padStart(2, "0")}`;

    possiblePaths.push(`./${datePath}/${justFilename}`);
    possiblePaths.push(`${datePath}/${justFilename}`);
  }

  for (let month = 1; month <= 12; month++) {
    for (let day = 1; day <= 31; day++) {
      const monthStr = month.toString().padStart(2, "0");
      const dayStr = day.toString().padStart(2, "0");

      const datePath = `${currentYear}/${monthStr}/${dayStr}`;

      possiblePaths.push(`./${datePath}/${justFilename}`);
      possiblePaths.push(`${datePath}/${justFilename}`);
    }
  }

  const year = currentDate.getFullYear();
  const month = currentDate.getMonth() + 1;
  const day = currentDate.getDate();

  possiblePaths.push(
    `./${year}-${month.toString().padStart(2, "0")}-${day
      .toString()
      .padStart(2, "0")}/${justFilename}`
  );
  possiblePaths.push(
    `${year}-${month.toString().padStart(2, "0")}-${day
      .toString()
      .padStart(2, "0")}/${justFilename}`
  );

  possiblePaths.push(`./${justFilename}`);
  possiblePaths.push(justFilename);

  possiblePaths.push(`./audio/${justFilename}`);
  possiblePaths.push(`./recordings/${justFilename}`);
  possiblePaths.push(`audio/${justFilename}`);
  possiblePaths.push(`recordings/${justFilename}`);

  return Array.from(new Set(possiblePaths));
}

async function downloadAudioFromSftp(filename: string): Promise<Buffer> {
  const sftpConfig = getSftpConfig();

  return new Promise<Buffer>((resolve, reject) => {
    const conn = new Client();
    let resolved = false;
    let sftpSession: any = null;

    const cleanup = () => {
      try {
        if (sftpSession) {
          sftpSession.end();
          sftpSession = null;
        }
        conn.end();
      } catch (e) {
        console.log("Cleanup error:", e);
      }
    };

    conn.on("ready", () => {
      console.log("SFTP connection ready for", filename);

      conn.sftp((err, sftp) => {
        if (err) {
          console.error("SFTP session error:", err);
          if (!resolved) {
            resolved = true;
            cleanup();
            reject(new Error("SFTP session error"));
          }
          return;
        }

        sftpSession = sftp;
        const possiblePaths = constructSftpPath(filename);
        console.log(`Searching ${possiblePaths.length} paths for ${filename}`);

        let pathIndex = 0;

        const tryNextPath = async () => {
          if (resolved) return;

          if (pathIndex >= possiblePaths.length) {
            console.error(`File not found in ${possiblePaths.length} paths`);
            if (!resolved) {
              resolved = true;
              cleanup();
              reject(new Error("Audio file not found"));
            }
            return;
          }

          const currentPath = possiblePaths[pathIndex];
          console.log(
            `Trying path ${pathIndex + 1}/${
              possiblePaths.length
            }: ${currentPath}`
          );

          try {
            const stats = await new Promise<any>(
              (resolveStats, rejectStats) => {
                sftp.stat(currentPath, (statErr, statsResult) => {
                  if (statErr) {
                    rejectStats(statErr);
                  } else {
                    resolveStats(statsResult);
                  }
                });
              }
            );

            const sizeInMB = stats.size / (1024 * 1024);
            console.log(`Found file: ${sizeInMB.toFixed(2)}MB`);

            if (stats.size === 0) {
              console.log(`Empty file, trying next`);
              pathIndex++;
              return tryNextPath();
            }

            if (stats.size < 10000) {
              console.log(`File too small: ${stats.size} bytes`);
              pathIndex++;
              return tryNextPath();
            }

            console.log(`Downloading ${stats.size} bytes`);

            const fileData = await new Promise<Buffer>(
              (resolveDownload, rejectDownload) => {
                const fileBuffers: Buffer[] = [];
                let totalBytesReceived = 0;

                const readStream = sftp.createReadStream(currentPath, {
                  highWaterMark: 256 * 1024,
                });

                readStream.on("error", (readErr: Error) => {
                  console.error(`Stream error: ${readErr.message}`);
                  rejectDownload(readErr);
                });

                readStream.on("data", (chunk: Buffer) => {
                  fileBuffers.push(chunk);
                  totalBytesReceived += chunk.length;

                  if (totalBytesReceived % (1024 * 1024) < chunk.length) {
                    const progress = (
                      (totalBytesReceived / stats.size) *
                      100
                    ).toFixed(0);
                    console.log(
                      `Progress: ${progress}% (${(
                        totalBytesReceived /
                        (1024 * 1024)
                      ).toFixed(1)}MB)`
                    );
                  }
                });

                readStream.on("end", () => {
                  const audioBuffer = Buffer.concat(fileBuffers);
                  console.log(`Downloaded: ${audioBuffer.length} bytes`);

                  if (audioBuffer.length !== stats.size) {
                    rejectDownload(
                      new Error(
                        `Size mismatch: expected ${stats.size}, got ${audioBuffer.length}`
                      )
                    );
                  } else {
                    resolveDownload(audioBuffer);
                  }
                });
              }
            );

            if (!resolved) {
              resolved = true;
              cleanup();
              resolve(fileData);
            }
            return;
          } catch (error) {
            console.log(
              `Error with path ${pathIndex + 1}: ${
                error instanceof Error ? error.message : "Unknown"
              }`
            );
            pathIndex++;
            setTimeout(tryNextPath, 200);
          }
        };

        tryNextPath();
      });
    });

    conn.on("error", (err) => {
      console.error("SFTP connection error:", err.message);
      if (!resolved) {
        resolved = true;
        cleanup();
        reject(new Error("SFTP connection failed"));
      }
    });

    try {
      conn.connect({
        ...sftpConfig,
        keepaliveInterval: 30000,
        keepaliveCountMax: 10,
        algorithms: {
          compress: ["none"],
        },
        tryKeyboard: false,
      });
    } catch (e) {
      console.error("Connection setup error:", e);
      if (!resolved) {
        resolved = true;
        reject(new Error("Failed to initialize SFTP connection"));
      }
    }
  });
}

async function uploadToAssemblyAI(
  audioBuffer: Buffer,
  apiKey: string
): Promise<string> {
  console.log("Uploading audio to AssemblyAI...");

  const uint8Array = new Uint8Array(audioBuffer);

  const uploadResponse = await fetch("https://api.assemblyai.com/v2/upload", {
    method: "POST",
    headers: {
      Authorization: apiKey,
      "Content-Type": "application/octet-stream",
    },
    body: uint8Array,
  });

  if (!uploadResponse.ok) {
    const errorText = await uploadResponse.text();
    throw new Error(
      `AssemblyAI upload failed: ${uploadResponse.status} - ${errorText}`
    );
  }

  const { upload_url } = await uploadResponse.json();
  console.log("Audio uploaded to AssemblyAI successfully");
  return upload_url;
}

// external categorisation
async function performTopicCategorization(transcriptData: any): Promise<{
  primary_category: string;
  topic_categories: string[];
  confidence: number;
} | null> {
  try {
    const serverUrl =
      process.env.NEXT_PUBLIC_SERVER_URL || "http://192.168.40.101";

    const response = await fetch(`${serverUrl}/api/openAI/categorise`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ transcript: transcriptData }),
      signal: AbortSignal.timeout(20000),
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

async function transcribeAudio(
  uploadUrl: string,
  speakerCount: number = 2
): Promise<any> {
  const apiKey = process.env.ASSEMBLYAI_API_KEY!;

  console.log("Submitting transcription to AssemblyAI...");

  const transcriptResponse = await fetch(
    "https://api.assemblyai.com/v2/transcript",
    {
      method: "POST",
      headers: {
        Authorization: apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        audio_url: uploadUrl,
        speech_model: "best",
        word_boost: [
          "Mycar",
          "Tyre",
          "And",
          "Auto",
          "Mycar - Tyre and Auto",
          "tyre",
          "tyres",
          "tire",
          "tires",
          "tread",
          "sidewall",
          "bead",
          "ply",
          "radial",
          "bias",
          "tubeless",
          "puncture",
          "blowout",
          "flat",
          "alignment",
          "balancing",
          "rotation",
          "pressure",
          "PSI",
          "inflation",
          "valve",
          "rim",
          "wheel",
          "alloy",
          "steel",
          "hubcap",
          "lug",
          "nuts",
          "Michelin",
          "Bridgestone",
          "Goodyear",
          "Continental",
          "Pirelli",
          "Yokohama",
          "Dunlop",
          "Hankook",
          "Toyo",
          "Kumho",
          "Maxxis",
          "BFGoodrich",
          "Falken",
          "sedan",
          "SUV",
          "4WD",
          "AWD",
          "coupe",
          "hatchback",
          "wagon",
          "brake",
          "brakes",
          "pads",
          "rotors",
          "suspension",
          "shocks",
          "struts",
          "exhaust",
          "muffler",
          "catalytic",
          "battery",
          "alternator",
          "starter",
          "radiator",
          "coolant",
          "thermostat",
          "servicing",
          "maintenance",
          "inspection",
          "diagnostic",
          "tune-up",
          "oil change",
          "filter",
          "fluids",
          "transmission",
          "differential",
          "axle",
          "CV joint",
          "bearings",
          "bushings",
          "gasket",
          "serpentine",
          "timing belt",
          "ABS",
          "ESP",
          "TCS",
          "TPMS",
          "ECU",
          "OBD",
          "VIN",
          "WOF",
          "rego",
          "registration",
          "warrant",
          "millimeter",
          "millimetres",
          "inches",
          "diameter",
          "width",
          "profile",
          "load index",
          "speed rating",
          "kilogram",
          "pound",
          "newton",
          "jack",
          "wrench",
          "spanner",
          "compressor",
          "gauge",
          "hoist",
          "torque",
          "hydraulic",
          "pneumatic",
        ],
        speaker_labels: true,
        speakers_expected: speakerCount,
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
    }
  );

  if (!transcriptResponse.ok) {
    const errorData = await transcriptResponse.json();
    throw new Error(
      `AssemblyAI submission failed: ${JSON.stringify(errorData)}`
    );
  }

  const { id } = await transcriptResponse.json();
  console.log(`Transcription job created: ${id}`);

  // polling stuff
  let transcript;
  let status = "processing";
  let attempts = 0;
  const maxAttempts = 120;
  const pollInterval = 5000;

  while (
    (status === "processing" || status === "queued") &&
    attempts < maxAttempts
  ) {
    await new Promise((resolve) => setTimeout(resolve, pollInterval));
    attempts++;

    const statusResponse = await fetch(
      `https://api.assemblyai.com/v2/transcript/${id}`,
      {
        headers: { Authorization: apiKey },
      }
    );

    if (!statusResponse.ok) {
      throw new Error(`Status check failed: ${statusResponse.status}`);
    }

    transcript = await statusResponse.json();
    status = transcript.status;

    if (attempts % 12 === 0) {
      console.log(`Transcription status after ${attempts * 5}s: ${status}`);
    }

    if (status === "completed" || status === "error") {
      break;
    }
  }

  if (status === "error") {
    throw new Error(
      `Transcription failed: ${transcript?.error || "Unknown error"}`
    );
  }

  if (status !== "completed") {
    throw new Error(
      `Transcription timed out after ${(maxAttempts * pollInterval) / 1000}s`
    );
  }

  console.log("Transcription completed successfully");

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

  return transcript;
}

async function saveTranscriptionToSupabase(
  callData: CallLog,
  transcriptData: any,
  categorization: any = null
): Promise<void> {
  try {
    const payload: TranscriptionData = {
      contact_id: callData.contact_id,
      recording_location: callData.recording_location || "",
      transcript_text: transcriptData.text || "",
      queue_name: callData.queue_name || "",
      agent_username: callData.agent_username || "",
      initiation_timestamp:
        callData.initiation_timestamp || new Date().toISOString(),
      speaker_data: transcriptData.utterances
        ? JSON.stringify(transcriptData.utterances)
        : null,
      sentiment_analysis: transcriptData.sentiment_analysis_results
        ? JSON.stringify(transcriptData.sentiment_analysis_results)
        : null,
      entities: transcriptData.entities
        ? JSON.stringify(transcriptData.entities)
        : null,
      disposition_title: callData.disposition_title || "",
      call_summary: transcriptData.summary || null,
      campaign_name: callData.campaign_name || null,
      campaign_id: callData.campaign_id?.toString() || null,
      customer_cli: callData.customer_cli || null,
      agent_hold_time: callData.agent_hold_time || null,
      total_hold_time: callData.total_hold_time || null,
      time_in_queue: callData.time_in_queue || null,
      call_duration: JSON.stringify(callData.total_call_time) || "",
      categories: categorization?.topic_categories
        ? JSON.stringify(categorization.topic_categories)
        : transcriptData.topic_categorization?.all_topics
        ? JSON.stringify(transcriptData.topic_categorization.all_topics)
        : null,
      primary_category:
        categorization?.primary_category ||
        transcriptData.topic_categorization?.primary_topic ||
        null,
    };

    console.log(
      "Saving transcription to Supabase for contact_id:",
      payload.contact_id
    );

    // does record exist
    const { data: existingRecord, error: checkError } = await supabase
      .from("call_records")
      .select("contact_id")
      .eq("contact_id", payload.contact_id)
      .single();

    if (checkError && checkError.code !== "PGRST116") {
      throw new Error(`Error checking existing record: ${checkError.message}`);
    }

    if (existingRecord) {
      const { error } = await supabase
        .from("call_records")
        .update(payload)
        .eq("contact_id", payload.contact_id);

      if (error) {
        throw new Error(`Failed to update record: ${error.message}`);
      }

      console.log("Successfully updated existing record");
    } else {
      const { error } = await supabase.from("call_records").insert([payload]);

      if (error) {
        throw new Error(`Failed to insert record: ${error.message}`);
      }

      console.log("Successfully inserted new record");
    }
  } catch (error) {
    console.error("Error saving to Supabase:", error);
    throw error;
  }
}

const processingCalls = new Set<string>();
const attemptedCalls = new Map<string, number>();
const MAX_ATTEMPTS = 3;

async function getFreshMissingTranscriptions(
  dateRange?: DateRange,
  maxCount?: number,
  excludeContactIds: string[] = []
): Promise<CallLog[]> {
  try {
    console.log("🔄 Getting fresh list of missing transcriptions...");
    console.log(
      `📋 Excluding ${excludeContactIds.length} contact IDs from client`
    );
    console.log(`🔒 Currently processing: ${processingCalls.size} calls`);
    console.log(`🚫 Failed attempts tracked: ${attemptedCalls.size} calls`);

    const logs = await getContactLogs(dateRange);
    console.log(`📊 Retrieved ${logs.length} total call logs from database`);

    const enhancedLogs = await enhanceCallLogsWithSupabaseStatus(logs);

    const alreadyInSupabase = enhancedLogs.filter(
      (log) => log.existsInSupabase
    ).length;
    console.log(`✅ ${alreadyInSupabase} calls already exist in Supabase`);

    const missingTranscriptions = enhancedLogs.filter((log) => {
      if (log.existsInSupabase) {
        return false;
      }

      if (!log.recording_location) {
        return false;
      }

      if (excludeContactIds.includes(log.contact_id)) {
        console.log(
          `⚠️ Excluding ${log.contact_id} - in client exclusion list`
        );
        return false;
      }

      if (processingCalls.has(log.contact_id)) {
        console.log(`⚠️ Excluding ${log.contact_id} - currently processing`);
        return false;
      }

      const attempts = attemptedCalls.get(log.contact_id) || 0;
      if (attempts >= MAX_ATTEMPTS) {
        console.log(
          `⚠️ Excluding ${log.contact_id} - exceeded ${MAX_ATTEMPTS} attempts (${attempts})`
        );
        return false;
      }

      return true;
    });

    console.log(
      `🎯 Found ${missingTranscriptions.length} genuinely missing transcriptions (after all exclusions)`
    );

    if (missingTranscriptions.length > 0) {
      const sampleIds = missingTranscriptions
        .slice(0, 5)
        .map((log) => log.contact_id);
      console.log(`📝 Sample missing contact IDs: ${sampleIds.join(", ")}`);
    }

    const result = maxCount
      ? missingTranscriptions.slice(0, maxCount)
      : missingTranscriptions;

    console.log(`📤 Returning ${result.length} calls for processing`);
    return result;
  } catch (error) {
    console.error("❌ Error getting fresh missing transcriptions:", error);
    return [];
  }
}

function markCallAsProcessing(contactId: string) {
  processingCalls.add(contactId);
  console.log(`🔒 Marked ${contactId} as processing`);
}

function unmarkCallAsProcessing(contactId: string, success: boolean = false) {
  processingCalls.delete(contactId);

  if (!success) {
    const attempts = (attemptedCalls.get(contactId) || 0) + 1;
    attemptedCalls.set(contactId, attempts);
    console.log(
      `❌ Unmarked ${contactId} as processing (attempt ${attempts}/${MAX_ATTEMPTS})`
    );
  } else {
    attemptedCalls.delete(contactId);
    console.log(`✅ Unmarked ${contactId} as processing (success)`);
  }
}

function clearProcessingState() {
  processingCalls.clear();
  attemptedCalls.clear();
  console.log("🧹 Cleared all processing state");
}

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const startDate = searchParams.get("startDate");
    const endDate = searchParams.get("endDate");
    const processTranscriptions =
      searchParams.get("processTranscriptions") === "true";
    const maxProcessCount = parseInt(
      searchParams.get("maxProcessCount") || "3"
    );
    const excludeContactIdsParam = searchParams.get("excludeContactIds");
    const excludeContactIds = excludeContactIdsParam
      ? excludeContactIdsParam.split(",").filter((id) => id.trim() !== "")
      : [];

    console.log("🚀 Starting unified call processing workflow");
    if (excludeContactIds.length > 0) {
      console.log(
        `📋 Excluding ${excludeContactIds.length} contact IDs from processing`
      );
    }

    let dateRange: DateRange | undefined;

    if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);

      if (isNaN(start.getTime()) || isNaN(end.getTime())) {
        return NextResponse.json(
          { error: "Invalid date format. Please use ISO date format." },
          { status: 400 }
        );
      }

      if (start > end) {
        return NextResponse.json(
          { error: "Start date must be before or equal to end date." },
          { status: 400 }
        );
      }

      dateRange = { start, end };
    }

    // get call logs
    console.log("📊 Step 1: Fetching call logs from database...");
    let logs = await getContactLogs(dateRange);
    console.log(`Found ${logs.length} call logs`);

    // check supabase
    console.log("🔍 Step 2: Checking Supabase for existing transcriptions...");
    logs = await enhanceCallLogsWithSupabaseStatus(logs);

    const missingTranscriptions = logs.filter(
      (log) => !log.existsInSupabase && log.recording_location
    );
    console.log(
      `Found ${missingTranscriptions.length} calls without transcriptions`
    );

    let processedCount = 0;
    const errors: any[] = [];
    const processedContactIds: string[] = [];

    // process missing transcripts
    if (processTranscriptions && missingTranscriptions.length > 0) {
      console.log(
        `🎵 Step 3: Processing up to ${maxProcessCount} missing transcriptions...`
      );

      const apiKey = process.env.ASSEMBLYAI_API_KEY;
      if (!apiKey) {
        return NextResponse.json(
          { error: "AssemblyAI API key not configured" },
          { status: 500 }
        );
      }

      const freshMissingTranscriptions = await getFreshMissingTranscriptions(
        dateRange,
        maxProcessCount,
        [...processedContactIds, ...excludeContactIds]
      );

      console.log(
        `🎯 After exclusions: ${freshMissingTranscriptions.length} calls to process`
      );

      if (freshMissingTranscriptions.length === 0) {
        console.log(
          `✅ No calls need processing after exclusions - all work complete`
        );

        const finalLogs = await enhanceCallLogsWithSupabaseStatus(logs);
        const finalSummary = {
          totalCalls: finalLogs.length,
          existingTranscriptions: finalLogs.filter(
            (log) => log.existsInSupabase
          ).length,
          missingTranscriptions: finalLogs.filter(
            (log) => !log.existsInSupabase && log.recording_location
          ).length,
          processedThisRequest: 0,
          errors: 0,
        };

        console.log(
          "🎉 No processing needed - returning current status:",
          finalSummary
        );
        clearProcessingState();

        return NextResponse.json({
          success: true,
          data: finalLogs,
          summary: finalSummary,
          processedContactIds: [],
          errors: undefined,
          dateRange: dateRange
            ? {
                start: dateRange.start.toISOString(),
                end: dateRange.end.toISOString(),
              }
            : null,
          timestamp: new Date().toISOString(),
        });
      }

      for (const log of freshMissingTranscriptions) {
        try {
          console.log(`\n🔍 Final check for call ${log.contact_id}...`);

          const stillMissing = !(await checkCallExistsInSupabase(
            log.contact_id
          ));
          if (!stillMissing) {
            console.log(
              `⏭️ Call ${log.contact_id} already processed by another batch, skipping...`
            );
            continue;
          }

          markCallAsProcessing(log.contact_id);

          console.log(`🎯 Processing call ${log.contact_id}...`);

          console.log("📥 Downloading audio from SFTP...");
          const audioBuffer = await downloadAudioFromSftp(
            log.recording_location
          );

          console.log("⬆️ Uploading to AssemblyAI...");
          const uploadUrl = await uploadToAssemblyAI(audioBuffer, apiKey);

          console.log("🎙️ Transcribing audio...");
          const transcript = await transcribeAudio(uploadUrl, 2);

          console.log("🏷️ Performing topic categorization...");
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
            }

            transcript.topic_categorization = categorization
              ? {
                  primary_topic: categorization.primary_category,
                  all_topics: categorization.topic_categories,
                  confidence: categorization.confidence,
                }
              : {
                  primary_topic: "Uncategorised",
                  all_topics: ["Uncategorised"],
                  confidence: 0,
                };
          }

          console.log("🔒 Final check before saving...");
          const finalCheck = await checkCallExistsInSupabase(log.contact_id);
          if (finalCheck) {
            console.log(
              `⚠️ Call ${log.contact_id} was processed by another batch during processing, skipping save...`
            );
            unmarkCallAsProcessing(log.contact_id, false);
            continue;
          }

          console.log("💾 Saving to Supabase...");
          await saveTranscriptionToSupabase(log, transcript, categorization);

          log.existsInSupabase = true;
          processedCount++;
          processedContactIds.push(log.contact_id);

          unmarkCallAsProcessing(log.contact_id, true);

          console.log(`✅ Successfully processed call ${log.contact_id}`);
        } catch (error) {
          console.error(`❌ Error processing call ${log.contact_id}:`, error);

          unmarkCallAsProcessing(log.contact_id, false);

          errors.push({
            contact_id: log.contact_id,
            error: error instanceof Error ? error.message : "Unknown error",
          });
        }
      }
    } else if (processTranscriptions && missingTranscriptions.length === 0) {
      console.log(
        "✅ No missing transcriptions found - all calls already processed"
      );
    }

    console.log("📋 Step 4: Getting final status after processing...");
    const finalLogs = await enhanceCallLogsWithSupabaseStatus(logs);

    const summary = {
      totalCalls: finalLogs.length,
      existingTranscriptions: finalLogs.filter((log) => log.existsInSupabase)
        .length,
      missingTranscriptions: finalLogs.filter(
        (log) => !log.existsInSupabase && log.recording_location
      ).length,
      processedThisRequest: processedCount,
      errors: errors.length,
    };

    console.log("🎉 Unified workflow completed:", summary);

    clearProcessingState();

    return NextResponse.json({
      success: true,
      data: finalLogs,
      summary,
      processedContactIds,
      errors: errors.length > 0 ? errors : undefined,
      dateRange: dateRange
        ? {
            start: dateRange.start.toISOString(),
            end: dateRange.end.toISOString(),
          }
        : null,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error("💥 Unified workflow error:", error);

    clearProcessingState();

    return NextResponse.json(
      {
        success: false,
        error: "Failed to process calls",
        details:
          process.env.NODE_ENV === "development" && error instanceof Error
            ? error.message
            : undefined,
      },
      { status: 500 }
    );
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { contactIds, processTranscriptions = true } = body;

    if (!contactIds || !Array.isArray(contactIds)) {
      return NextResponse.json(
        { error: "contactIds array is required" },
        { status: 400 }
      );
    }

    console.log(`🚀 Processing specific calls: ${contactIds.join(", ")}`);

    const logs = await getContactLogs();
    const targetLogs = logs.filter((log) =>
      contactIds.includes(log.contact_id)
    );

    if (targetLogs.length === 0) {
      return NextResponse.json(
        { error: "No matching call logs found" },
        { status: 404 }
      );
    }

    const enhancedLogs = await enhanceCallLogsWithSupabaseStatus(targetLogs);
    const missingTranscriptions = enhancedLogs.filter(
      (log) => !log.existsInSupabase && log.recording_location
    );

    let processedCount = 0;
    const errors: any[] = [];

    if (processTranscriptions && missingTranscriptions.length > 0) {
      const apiKey = process.env.ASSEMBLYAI_API_KEY;
      if (!apiKey) {
        return NextResponse.json(
          { error: "AssemblyAI API key not configured" },
          { status: 500 }
        );
      }

      for (const log of missingTranscriptions) {
        try {
          console.log(`🔍 Final check for call ${log.contact_id}...`);

          const stillMissing = !(await checkCallExistsInSupabase(
            log.contact_id
          ));
          if (!stillMissing) {
            console.log(
              `⏭️ Call ${log.contact_id} already processed, skipping...`
            );
            continue;
          }

          markCallAsProcessing(log.contact_id);

          console.log(`🎯 Processing call ${log.contact_id}...`);

          const audioBuffer = await downloadAudioFromSftp(
            log.recording_location
          );
          const uploadUrl = await uploadToAssemblyAI(audioBuffer, apiKey);
          const transcript = await transcribeAudio(uploadUrl, 2);

          let categorization = null;
          if (transcript.utterances && transcript.utterances.length > 0) {
            try {
              categorization = await performTopicCategorization(transcript);
            } catch (catError) {
              console.error("Categorization failed:", catError);
            }

            transcript.topic_categorization = categorization
              ? {
                  primary_topic: categorization.primary_category,
                  all_topics: categorization.topic_categories,
                  confidence: categorization.confidence,
                }
              : {
                  primary_topic: "Uncategorised",
                  all_topics: ["Uncategorised"],
                  confidence: 0,
                };
          }

          const finalCheck = await checkCallExistsInSupabase(log.contact_id);
          if (finalCheck) {
            console.log(
              `⚠️ Call ${log.contact_id} was processed during processing, skipping save...`
            );
            unmarkCallAsProcessing(log.contact_id, false);
            continue;
          }

          await saveTranscriptionToSupabase(log, transcript, categorization);
          log.existsInSupabase = true;
          processedCount++;

          unmarkCallAsProcessing(log.contact_id, true);

          console.log(`✅ Successfully processed call ${log.contact_id}`);
        } catch (error) {
          console.error(`❌ Error processing call ${log.contact_id}:`, error);

          unmarkCallAsProcessing(log.contact_id, false);

          errors.push({
            contact_id: log.contact_id,
            error: error instanceof Error ? error.message : "Unknown error",
          });
        }
      }
    }

    // Get final status
    const finalEnhancedLogs = await enhanceCallLogsWithSupabaseStatus(
      enhancedLogs
    );

    clearProcessingState();

    return NextResponse.json({
      success: true,
      data: finalEnhancedLogs,
      summary: {
        requestedCalls: contactIds.length,
        foundCalls: targetLogs.length,
        existingTranscriptions: finalEnhancedLogs.filter(
          (log) => log.existsInSupabase
        ).length,
        processedThisRequest: processedCount,
        errors: errors.length,
      },
      errors: errors.length > 0 ? errors : undefined,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Error in POST workflow:", error);

    // reset state
    clearProcessingState();

    return NextResponse.json(
      {
        success: false,
        error: "Failed to process specific calls",
        details:
          process.env.NODE_ENV === "development" && error instanceof Error
            ? error.message
            : undefined,
      },
      { status: 500 }
    );
  }
}

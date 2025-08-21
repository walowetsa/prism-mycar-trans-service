// lib/supabaseUtils.ts
import { supabase } from './supabase';

export interface CallLogWithSupabaseStatus {
  contact_id: string;
  agent_username: string;
  recording_location: string;
  initiation_timestamp: string;
  total_call_time: {
    minutes: number;
    seconds: number;
  };
  queue_name: string;
  disposition_title: string;
  existsInSupabase?: boolean; // New field to track Supabase status
}

// Function to split array into chunks
function chunkArray<T>(array: T[], chunkSize: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
}

// Function to check if contact IDs exist in Supabase (with batching)
export async function checkContactIdsInSupabase(contactIds: string[]): Promise<string[]> {
  try {
    if (contactIds.length === 0) return [];

    // Split contact IDs into chunks to avoid URL length limits
    const BATCH_SIZE = 100; // Adjust this size based on your needs
    const chunks = chunkArray(contactIds, BATCH_SIZE);
    const allExistingIds: string[] = [];

    // Process each chunk
    for (const chunk of chunks) {
      const { data, error } = await supabase
        .from('transcriptions') // Replace with your actual table name
        .select('contact_id')
        .in('contact_id', chunk);

      if (error) {
        console.error('Error checking contact IDs in Supabase:', error);
        // Continue with other chunks even if one fails
        continue;
      }

      // Add found IDs to our result array
      if (data) {
        allExistingIds.push(...data.map(row => row.contact_id));
      }
    }

    return allExistingIds;
  } catch (error) {
    console.error('Error in checkContactIdsInSupabase:', error);
    return [];
  }
}

// Function to enhance call logs with Supabase status
export async function enhanceCallLogsWithSupabaseStatus(
  callLogs: CallLogWithSupabaseStatus[]
): Promise<CallLogWithSupabaseStatus[]> {
  if (callLogs.length === 0) return callLogs;

  // Extract all contact IDs
  const contactIds = callLogs.map(log => log.contact_id);
  
  // Check which ones exist in Supabase
  const existingIds = await checkContactIdsInSupabase(contactIds);
  const existingIdsSet = new Set(existingIds);

  // Add the existsInSupabase property to each log
  return callLogs.map(log => ({
    ...log,
    existsInSupabase: existingIdsSet.has(log.contact_id)
  }));
}
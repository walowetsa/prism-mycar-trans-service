import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

// Initialize Supabase client
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;

if (!supabaseUrl || !supabaseServiceKey) {
  throw new Error('Missing Supabase environment variables');
}

const supabase = createClient(supabaseUrl, supabaseServiceKey);

// Define the interface for the transcription data
interface TranscriptionData {
  call_duration: string
  contact_id: string;
  recording_location: string;
  transcript_text: string;
  queue_name?: string;
  agent_username: string;
  initiation_timestamp: string;
  speaker_data?: string | null;
  sentiment_analysis?: string | null;
  entities?: string | null;
  categories?: string | null; // This will be the JSONB categories column
  disposition_title?: string;
  call_summary?: string | null;
  campaign_name?: string | null;
  campaign_id?: string | null;
  customer_cli?: string | null;
  agent_hold_time?: number | null;
  total_hold_time?: number | null;
  time_in_queue?: number | null;
  primary_category?: string | null; // This will be the text primary_category column
}

export async function POST(request: NextRequest) {
  try {
    // Parse the request body
    const transcriptionData: TranscriptionData = await request.json();

    // Log the received data for debugging
    console.log('Received transcription data:', {
      contact_id: transcriptionData.contact_id,
      has_transcript: !!transcriptionData.transcript_text,
      transcript_length: transcriptionData.transcript_text?.length || 0,
      has_categories: !!transcriptionData.categories,
      primary_category: transcriptionData.primary_category,
      categories_preview: transcriptionData.categories ? transcriptionData.categories.substring(0, 100) : null
    });

    // Validate required fields
    if (!transcriptionData.contact_id) {
      return NextResponse.json(
        { error: 'Missing required field: contact_id' },
        { status: 400 }
      );
    }

    if (!transcriptionData.recording_location) {
      return NextResponse.json(
        { error: 'Missing required field: recording_location' },
        { status: 400 }
      );
    }

    if (!transcriptionData.agent_username) {
      return NextResponse.json(
        { error: 'Missing required field: agent_username' },
        { status: 400 }
      );
    }

    if (!transcriptionData.initiation_timestamp) {
      return NextResponse.json(
        { error: 'Missing required field: initiation_timestamp' },
        { status: 400 }
      );
    }

    // Check if record already exists
    const { data: existingRecord, error: checkError } = await supabase
      .from('call_records') // Replace with your actual table name
      .select('contact_id')
      .eq('contact_id', transcriptionData.contact_id)
      .single();

    if (checkError && checkError.code !== 'PGRST116') {
      // PGRST116 is "not found" error, which is expected if record doesn't exist
      console.error('Error checking existing record:', checkError);
      return NextResponse.json(
        { error: 'Database error while checking existing record' },
        { status: 500 }
      );
    }

    const baseData = {
      recording_location: transcriptionData.recording_location,
      transcript_text: transcriptionData.transcript_text,
      queue_name: transcriptionData.queue_name,
      agent_username: transcriptionData.agent_username,
      initiation_timestamp: transcriptionData.initiation_timestamp,
      speaker_data: transcriptionData.speaker_data,
      sentiment_analysis: transcriptionData.sentiment_analysis,
      entities: transcriptionData.entities,
      categories: transcriptionData.categories, // JSONB column for all categories
      disposition_title: transcriptionData.disposition_title,
      call_summary: transcriptionData.call_summary,
      campaign_name: transcriptionData.campaign_name,
      campaign_id: transcriptionData.campaign_id,
      customer_cli: transcriptionData.customer_cli,
      agent_hold_time: transcriptionData.agent_hold_time,
      total_hold_time: transcriptionData.total_hold_time,
      time_in_queue: transcriptionData.time_in_queue,
      call_duration: transcriptionData.call_duration,
      primary_category: transcriptionData.primary_category, // Text column for primary category
    };

    // Log what we're about to save for debugging
    console.log('Saving to Supabase with categories:', {
      contact_id: transcriptionData.contact_id,
      categories_data: baseData.categories,
      primary_category_data: baseData.primary_category
    });

    if (existingRecord) {
      // Record already exists, update it instead
      console.log(`Updating existing record for contact_id: ${transcriptionData.contact_id}`);
      
      const { data, error } = await supabase
        .from('call_records')
        .update(baseData)
        .eq('contact_id', transcriptionData.contact_id)
        .select();

      if (error) {
        console.error('Supabase update error:', error);
        return NextResponse.json(
          { error: 'Failed to update transcription data in database', details: error.message },
          { status: 500 }
        );
      }

      console.log('Successfully updated record with categories:', {
        contact_id: transcriptionData.contact_id,
        categories: data[0]?.categories,
        primary_category: data[0]?.primary_category
      });

      return NextResponse.json({
        success: true,
        message: 'Transcription data updated successfully',
        data: data[0],
        operation: 'update'
      });

    } else {
      // Record doesn't exist, insert new one
      console.log(`Inserting new record for contact_id: ${transcriptionData.contact_id}`);
      
      const { data, error } = await supabase
        .from('call_records')
        .insert([{
          contact_id: transcriptionData.contact_id,
          ...baseData
        }])
        .select();

      if (error) {
        console.error('Supabase insert error:', error);
        return NextResponse.json(
          { error: 'Failed to save transcription data to database', details: error.message },
          { status: 500 }
        );
      }

      console.log('Successfully inserted record with categories:', {
        contact_id: transcriptionData.contact_id,
        categories: data[0]?.categories,
        primary_category: data[0]?.primary_category
      });

      return NextResponse.json({
        success: true,
        message: 'Transcription data saved successfully',
        data: data[0],
        operation: 'insert'
      });
    }

  } catch (error) {
    console.error('Error in save-transcription API:', error);
    return NextResponse.json(
      { 
        error: 'Internal server error', 
        details: error instanceof Error ? error.message : 'Unknown error' 
      },
      { status: 500 }
    );
  }
}

// Optional: Add GET method to retrieve transcription data
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const contactId = searchParams.get('contact_id');

    if (!contactId) {
      return NextResponse.json(
        { error: 'Missing contact_id parameter' },
        { status: 400 }
      );
    }

    const { data, error } = await supabase
      .from('call_records') // Replace with your actual table name
      .select('*')
      .eq('contact_id', contactId)
      .single();

    if (error) {
      if (error.code === 'PGRST116') {
        return NextResponse.json(
          { error: 'Transcription not found' },
          { status: 404 }
        );
      }
      console.error('Supabase select error:', error);
      return NextResponse.json(
        { error: 'Failed to retrieve transcription data', details: error.message },
        { status: 500 }
      );
    }

    return NextResponse.json({
      success: true,
      data: data
    });

  } catch (error) {
    console.error('Error in GET save-transcription API:', error);
    return NextResponse.json(
      { 
        error: 'Internal server error', 
        details: error instanceof Error ? error.message : 'Unknown error' 
      },
      { status: 500 }
    );
  }
}
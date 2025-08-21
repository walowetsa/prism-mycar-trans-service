import { NextRequest, NextResponse } from 'next/server';
import { getContactLogs } from '@/lib/db';
import { enhanceCallLogsWithSupabaseStatus } from '@/lib/supabaseUtils';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const startDate = searchParams.get('startDate');
    const endDate = searchParams.get('endDate');
    const checkSupabase = searchParams.get('checkSupabase') === 'true'; // Optional parameter

    let dateRange;
    
    if (startDate && endDate) {
      // Parse the dates from the query parameters
      const start = new Date(startDate);
      const end = new Date(endDate);
      
      // Validate the dates
      if (isNaN(start.getTime()) || isNaN(end.getTime())) {
        return NextResponse.json(
          { error: 'Invalid date format. Please use ISO date format.' },
          { status: 400 }
        );
      }
      
      // Ensure start date is before end date
      if (start > end) {
        return NextResponse.json(
          { error: 'Start date must be before or equal to end date.' },
          { status: 400 }
        );
      }
      
      dateRange = { start, end };
    }

    // Get the contact logs with optional date filtering
    let logs = await getContactLogs();
    
    // Enhance with Supabase status if requested
    if (checkSupabase) {
      logs = await enhanceCallLogsWithSupabaseStatus(logs);
    }
    
    return NextResponse.json({
      success: true,
      data: logs,
      count: logs.length,
      dateRange: dateRange ? {
        start: dateRange.start.toISOString(),
        end: dateRange.end.toISOString()
      } : null,
      supabaseChecked: checkSupabase
    });
    
  } catch (error) {
    console.error('API Error:', error);
    return NextResponse.json(
      { 
        success: false, 
        error: 'Failed to fetch call logs',
        details: process.env.NODE_ENV === 'development' && error instanceof Error ? error.message : undefined
      },
      { status: 500 }
    );
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { startDate, endDate, checkSupabase = false } = body;

    let dateRange;
    
    if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);
      
      if (isNaN(start.getTime()) || isNaN(end.getTime())) {
        return NextResponse.json(
          { error: 'Invalid date format. Please use ISO date format.' },
          { status: 400 }
        );
      }
      
      if (start > end) {
        return NextResponse.json(
          { error: 'Start date must be before or equal to end date.' },
          { status: 400 }
        );
      }
      
      dateRange = { start, end };
    }

    let logs = await getContactLogs();
    
    // Enhance with Supabase status if requested
    if (checkSupabase) {
      logs = await enhanceCallLogsWithSupabaseStatus(logs);
    }
    
    return NextResponse.json({
      success: true,
      data: logs,
      count: logs.length,
      dateRange: dateRange ? {
        start: dateRange.start.toISOString(),
        end: dateRange.end.toISOString()
      } : null,
      supabaseChecked: checkSupabase
    });
    
  } catch (error) {
    console.error('API Error:', error);
    return NextResponse.json(
      { 
        success: false, 
        error: 'Failed to fetch call logs',
        details: process.env.NODE_ENV === 'development' && error instanceof Error ? error.message : undefined
      },
      { status: 500 }
    );
  }
}
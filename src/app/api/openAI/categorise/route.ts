/* eslint-disable @typescript-eslint/no-explicit-any */
import { NextResponse } from "next/server";
import OpenAI from "openai";

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// TODO: Update Categories - Mycar Appropriate
const TOPIC_CATEGORIES = [
  "No Lead - Call Refused",
  "Lead Generated - New Business",
  "No Lead - No Product Service Match",
  "Other",
];

export async function POST(request: Request) {
  try {
    console.log("Categorization API called");
    
    const body = await request.json();
    const { transcript } = body;

    if (!transcript) {
      console.error("No transcript data provided");
      return NextResponse.json(
        { error: "Transcript data is required" },
        { status: 400 },
      );
    }

    console.log("Transcript received:", {
      hasUtterances: !!(transcript.utterances && transcript.utterances.length > 0),
      utteranceCount: transcript.utterances?.length || 0,
      hasText: !!transcript.text,
      textLength: transcript.text?.length || 0
    });

    // Check if we have utterances to work with
    if (!transcript.utterances || transcript.utterances.length === 0) {
      console.error("No utterances found in transcript");
      return NextResponse.json(
        { error: "No utterances found in transcript data" },
        { status: 400 },
      );
    }

    const formattedTranscript = transcript.utterances
      .map((u: any) => `${u.speakerRole || u.speaker}: ${u.text}`)
      .join("\n");

    console.log("Formatted transcript for OpenAI:", {
      length: formattedTranscript.length,
      preview: formattedTranscript.substring(0, 200) + "..."
    });

    // Check if OpenAI API key is available
    if (!process.env.OPENAI_API_KEY) {
      console.error("OpenAI API key not configured");
      return NextResponse.json(
        { error: "OpenAI API key not configured" },
        { status: 500 },
      );
    }

    console.log("Sending request to OpenAI...");

    // POST to OpenAI for multi-topic categorization
    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        {
          role: "system",
          content: `You are a specialized AI that categorizes customer service call transcripts into specific topics.
          
          Analyze the following call transcript and identify the 1-3 MOST RELEVANT categories from this list:
          ${TOPIC_CATEGORIES.join(", ")}
          
          Respond with ONLY the category names separated by "||". Include ONLY 1-3 categories based on relevance - if only one topic is clearly relevant, return just that one.
          
          Example responses:
          "No Lead - Call Refused"
          "No Lead - No Product Service Match" 
          "Other"
          
          Here is the call transcript:
          ${formattedTranscript}`,
        },
      ],
      max_tokens: 50,
      temperature: 0.3,
    });

    console.log("OpenAI response received");

    const categoriesResponse = completion.choices[0].message.content?.trim() || "";
    console.log("Raw OpenAI response:", categoriesResponse);
    
    // Split the response and filter for valid categories
    const categoriesArray = categoriesResponse.split("||")
      .map(category => category.trim())
      .filter(category => TOPIC_CATEGORIES.includes(category));
    
    console.log("Parsed categories:", categoriesArray);
    
    // Fallback to "Other" if no valid categories were returned
    const validCategories = categoriesArray.length > 0 ? categoriesArray : ["Other"];
    
    // Limit to maximum 3 categories
    const finalCategories = validCategories.slice(0, 3);

    console.log("Final categories:", finalCategories);

    const response = { 
      topic_categories: finalCategories,
      primary_category: finalCategories[0],
      confidence: categoriesArray.length > 0 ? 1.0 : 0.0
    };

    console.log("Sending categorization response:", response);

    return NextResponse.json(response);
  } catch (error: any) {
    console.error("Error in topic categorization API route:", error);
    
    // Return a more detailed error response
    return NextResponse.json(
      { 
        error: "Internal server error", 
        message: error.message,
        details: error.stack
      },
      { status: 500 },
    );
  }
}
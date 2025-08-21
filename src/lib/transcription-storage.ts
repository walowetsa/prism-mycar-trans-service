import fs from 'fs';
import path from 'path';

// Define the path for storing transcriptions
const TRANSCRIPTION_DIR = path.join(process.cwd(), 'call_records');

// Ensure the directory exists
if (!fs.existsSync(TRANSCRIPTION_DIR)) {
  fs.mkdirSync(TRANSCRIPTION_DIR, { recursive: true });
}

// In-memory cache for transcription existence checks
interface CacheEntry {
  exists: boolean;
  timestamp: number;
}

const existsCache = new Map<string, CacheEntry>();
const CACHE_DURATION = 5 * 60 * 1000; // 5 minutes cache validity

// Cache for full transcription content
const transcriptionCache = new Map<string, {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any;
  timestamp: number;
}>();

// Function to extract the base filename without the path
const extractBaseFilename = (filePath: string): string => {
  // Get just the filename part, regardless of path structure
  return path.basename(filePath);
};

// Function to generate a filename for a transcription
export const getTranscriptionFilename = (audioFilename: string): string => {
  // Extract just the base filename without any path
  const baseFilename = extractBaseFilename(audioFilename);
  return path.join(TRANSCRIPTION_DIR, `${baseFilename}.json`);
};

// Function to save a transcription
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const saveTranscription = (audioFilename: string, transcription: any): void => {
  const filePath = getTranscriptionFilename(audioFilename);
  fs.writeFileSync(filePath, JSON.stringify(transcription, null, 2));
  
  // Update the caches when we save a new transcription
  const baseFilename = extractBaseFilename(audioFilename);
  existsCache.set(baseFilename, { exists: true, timestamp: Date.now() });
  transcriptionCache.set(baseFilename, { data: transcription, timestamp: Date.now() });
};

// Manage cache size to prevent memory leaks
const cleanupCache = () => {
  const now = Date.now();
  
  // Clean up exists cache
  if (existsCache.size > 1000) {
    for (const [key, value] of existsCache.entries()) {
      if (now - value.timestamp > CACHE_DURATION) {
        existsCache.delete(key);
      }
    }
  }
  
  // Clean up transcription cache
  if (transcriptionCache.size > 200) { // Keep transcription cache smaller as it holds more data
    for (const [key, value] of transcriptionCache.entries()) {
      if (now - value.timestamp > CACHE_DURATION) {
        transcriptionCache.delete(key);
      }
    }
  }
};

// Function to check if a transcription exists with caching
export const transcriptionExists = (audioFilename: string): boolean => {
  const baseFilename = extractBaseFilename(audioFilename);
  const now = Date.now();
  
  // Check cache first
  const cachedResult = existsCache.get(baseFilename);
  if (cachedResult && now - cachedResult.timestamp < CACHE_DURATION) {
    return cachedResult.exists;
  }
  
  // If not in cache or cache expired, check filesystem
  const filePath = getTranscriptionFilename(audioFilename);
  const exists = fs.existsSync(filePath);
  
  // Update cache with result
  existsCache.set(baseFilename, { exists, timestamp: now });
  
  // Occasionally clean up old cache entries
  if (existsCache.size % 100 === 0) {
    cleanupCache();
  }
  
  return exists;
};

// Asynchronous version of transcription exists to avoid blocking
export const transcriptionExistsAsync = async (audioFilename: string): Promise<boolean> => {
  const baseFilename = extractBaseFilename(audioFilename);
  const now = Date.now();
  
  // Check cache first
  const cachedResult = existsCache.get(baseFilename);
  if (cachedResult && now - cachedResult.timestamp < CACHE_DURATION) {
    return cachedResult.exists;
  }
  
  // If not in cache or cache expired, check filesystem asynchronously
  const filePath = getTranscriptionFilename(audioFilename);
  
  try {
    await fs.promises.access(filePath, fs.constants.F_OK);
    existsCache.set(baseFilename, { exists: true, timestamp: now });
    return true;
  } catch {
    existsCache.set(baseFilename, { exists: false, timestamp: now });
    return false;
  } finally {
    // Occasionally clean up old cache entries
    if (existsCache.size % 100 === 0) {
      cleanupCache();
    }
  }
};

// Function to get a transcription if it exists with caching
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const getTranscription = (audioFilename: string): any | null => {
  const baseFilename = extractBaseFilename(audioFilename);
  const now = Date.now();
  
  // Check cache first
  const cachedTranscription = transcriptionCache.get(baseFilename);
  if (cachedTranscription && now - cachedTranscription.timestamp < CACHE_DURATION) {
    return cachedTranscription.data;
  }
  
  // If not in cache or cache expired, check filesystem
  const filePath = getTranscriptionFilename(audioFilename);
  
  if (fs.existsSync(filePath)) {
    try {
      const data = fs.readFileSync(filePath, 'utf8');
      const parsedData = JSON.parse(data);
      
      // Update cache
      transcriptionCache.set(baseFilename, { data: parsedData, timestamp: now });
      
      return parsedData;
    } catch (error) {
      console.error('Error reading transcription file:', error);
      return null;
    }
  }
  
  return null;
};

// Async version to get a transcription
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const getTranscriptionAsync = async (audioFilename: string): Promise<any | null> => {
  const baseFilename = extractBaseFilename(audioFilename);
  const now = Date.now();
  
  // Check cache first
  const cachedTranscription = transcriptionCache.get(baseFilename);
  if (cachedTranscription && now - cachedTranscription.timestamp < CACHE_DURATION) {
    return cachedTranscription.data;
  }
  
  // If not in cache or cache expired, check filesystem asynchronously
  const filePath = getTranscriptionFilename(audioFilename);
  
  try {
    const exists = await transcriptionExistsAsync(audioFilename);
    if (!exists) return null;
    
    const data = await fs.promises.readFile(filePath, 'utf8');
    const parsedData = JSON.parse(data);
    
    // Update cache
    transcriptionCache.set(baseFilename, { data: parsedData, timestamp: now });
    
    return parsedData;
  } catch (error) {
    console.error('Error reading transcription file:', error);
    return null;
  }
};
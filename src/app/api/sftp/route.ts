import { Client } from "ssh2";
import { NextResponse } from "next/server";
import { readFileSync } from "fs";
import * as path from "path";

// Define the SFTP connection configuration type
type SftpConfig = {
  host: string;
  port: number;
  username: string;
  privateKey: Buffer;
  passphrase: string;
};

// Define the file info type
type FileInfo = {
  filename: string;
  size: number;
  modifyTime: string;
  path: string;
  year: string;
  month: string;
  day: string;
};

// Define the SFTP configuration
const sftpConfig: SftpConfig = {
  host: process.env.SFTP_HOST!,
  port: parseInt(process.env.SFTP_PORT!),
  username: process.env.SFTP_USERNAME!,
  privateKey: readFileSync(
    path.resolve(process.env.HOME || "~", ".ssh/sftp_key")
  ),
  passphrase: process.env.SFTP_PASSPHRASE!,
};

// Get directories for current year and all months up to current month
const getDirectoriesToScan = () => {
  const currentDate = new Date();
  const currentYear = currentDate.getFullYear();
  const currentMonth = currentDate.getMonth() + 1; // getMonth() returns 0-11
  const currentDay = currentDate.getDate();
  
  const directories = [];
  const baseDir = `./${currentYear}`;
  
  // Scan all months from January up to current month
  for (let month = 1; month <= currentMonth; month++) {
    const monthStr = month.toString().padStart(2, '0');
    
    // For current month, only scan up to current day
    // For past months, scan all possible days (1-31)
    const maxDay = month === currentMonth ? currentDay : 31;
    
    for (let day = 1; day <= maxDay; day++) {
      const dayStr = day.toString().padStart(2, '0');
      directories.push(`${baseDir}/${monthStr}/${dayStr}`);
    }
  }
  
  console.log(`Scanning ${directories.length} directories from ${currentYear}/01/01 to ${currentYear}/${currentMonth.toString().padStart(2, '0')}/${currentDay.toString().padStart(2, '0')}`);
  return directories;
};

// API Route Handler
export async function GET() {
  return new Promise<NextResponse>((resolve, reject) => {
    const conn = new Client();
    const allFiles: FileInfo[] = [];
    let pendingDirectories = 0;
    const directoriesToScan = getDirectoriesToScan();
    
    conn.on("ready", () => {
      console.log("SFTP connection ready.");

      conn.sftp((err, sftp) => {
        if (err) {
          return reject(
            NextResponse.json(
              { error: "SFTP connection error" },
              { status: 500 }
            )
          );
        }

        // Track how many directories we've processed
        pendingDirectories = directoriesToScan.length;
        
        // If no directories to scan, return empty result
        if (pendingDirectories === 0) {
          resolve(NextResponse.json({ files: [] }, { status: 200 }));
          conn.end();
          return;
        }
        
        // Process each directory
        directoriesToScan.forEach((directory) => {
          // List files in each directory
          sftp.readdir(directory, (dirErr, list) => {
            pendingDirectories--;
            
            // Skip directories that don't exist or have errors
            if (dirErr) {
              console.log(`Directory not found or error: ${directory}`);
              
              // If we've processed all directories, return the results
              if (pendingDirectories === 0) {
                resolve(NextResponse.json({ files: allFiles }, { status: 200 }));
                conn.end();
              }
              return;
            }
            
            // Extract date components from the directory path
            // Expected format: ./2025/MM/DD
            const pathParts = directory.split('/');
            const year = pathParts[pathParts.length - 3];
            const month = pathParts[pathParts.length - 2];
            const day = pathParts[pathParts.length - 1];
            
            // Format the file list to include only .wav files
            const dirFiles = list
              .filter(
                (item) =>
                  item.attrs.isFile() && item.filename.toLowerCase().endsWith(".wav")
              )
              .map((item) => ({
                filename: item.filename,
                size: item.attrs.size,
                modifyTime: new Date(item.attrs.mtime * 1000).toISOString(),
                path: `${directory}/${item.filename}`,
                // Add date information to help with sorting/filtering
                year,
                month,
                day
              }));
            
            // Add this directory's files to our collection
            allFiles.push(...dirFiles);
            
            console.log(`Found ${dirFiles.length} .wav files in ${directory}`);
            
            // If we've processed all directories, return the results
            if (pendingDirectories === 0) {
              // Sort files by date (newest first)
              allFiles.sort((a, b) => {
                return new Date(b.modifyTime).getTime() - new Date(a.modifyTime).getTime();
              });
              
              console.log(`Total files found: ${allFiles.length}`);
              resolve(NextResponse.json({ files: allFiles }, { status: 200 }));
              conn.end();
            }
          });
        });
      });
    });

    // Handle any errors during the connection
    conn.on("error", (err) => {
      console.error("Connection error:", err);
      reject(NextResponse.json({ error: "Connection error" }, { status: 500 }));
    });

    // Establish the connection
    conn.connect(sftpConfig);
  });
}

// Download route to get a specific file by path
export async function POST(request: Request) {
  const { path: filePath } = await request.json();
  
  if (!filePath) {
    return NextResponse.json({ error: "File path is required" }, { status: 400 });
  }
  
  // Validate that the path follows the expected structure
  const pathRegex = /^\.\/\d{4}\/\d{2}\/\d{2}\/.*\.wav$/i;
  if (!pathRegex.test(filePath)) {
    console.warn(`File path doesn't match expected structure: ${filePath}`);
    // Still proceed, but log the warning
  }
  
  return new Promise<NextResponse>((resolve, reject) => {
    const conn = new Client();
    
    conn.on("ready", () => {
      conn.sftp((err, sftp) => {
        if (err) {
          return reject(
            NextResponse.json(
              { error: "SFTP connection error" },
              { status: 500 }
            )
          );
        }
        
        // Get file stream
        const readStream = sftp.createReadStream(filePath);
        const fileData: Buffer[] = [];
        
        readStream.on('data', (chunk: Buffer) => {
          fileData.push(chunk);
        });
        
        readStream.on('end', () => {
          const fileBuffer = Buffer.concat(fileData);
          
          // Extract filename from path
          const fileName = filePath.split('/').pop() || 'download.wav';
          
          // Return file with appropriate headers for WAV files
          resolve(
            new NextResponse(fileBuffer, {
              status: 200,
              headers: {
                'Content-Type': 'audio/wav',
                'Content-Disposition': `attachment; filename="${fileName}"`,
              },
            })
          );
          
          conn.end();
        });
        
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        readStream.on('error', (fileErr: { message: any; }) => {
          console.error(`Error reading file ${filePath}:`, fileErr);
          reject(
            NextResponse.json(
              { error: `Error reading file: ${fileErr.message}` },
              { status: 500 }
            )
          );
          conn.end();
        });
      });
    });
    
    conn.on("error", (err) => {
      console.error("Connection error:", err);
      reject(NextResponse.json({ error: "Connection error" }, { status: 500 }));
    });
    
    conn.connect(sftpConfig);
  });
}
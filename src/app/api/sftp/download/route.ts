/* eslint-disable @typescript-eslint/no-unused-vars */
// Improved path construction with broader search
// eslint-disable-next-line @typescript-eslint/no-unused-vars
function constructSftpPath(filename: string): string[] {
  const possiblePaths = [];
  
  // Decode URL encoding
  let decodedFilename = filename;
  try {
    decodedFilename = decodeURIComponent(filename);
  } catch (error) {
    console.log(`⚠️ Could not decode filename: ${filename}`);
  }
  
  // If filename has path structure, use it first
  if (decodedFilename.includes('/')) {
    let cleanPath = decodedFilename;
    
    // Handle known prefixes
    if (cleanPath.startsWith('amazon-connect-b1a9c08821e5/')) {
      cleanPath = cleanPath.replace('amazon-connect-b1a9c08821e5/', '');
    }
    
    if (!cleanPath.startsWith('./') && !cleanPath.startsWith('/')) {
      cleanPath = `./${cleanPath}`;
    }
    
    possiblePaths.push(cleanPath);
    
    // Also try without leading ./
    if (cleanPath.startsWith('./')) {
      possiblePaths.push(cleanPath.substring(2));
    }
  }
  
  // Extract filename for date-based searches
  const justFilename = decodedFilename.split('/').pop() || decodedFilename;
  
  // COMPREHENSIVE DATE SEARCH: Current year with all months/days + 1 day forward
  const currentDate = new Date();
  const currentYear = currentDate.getFullYear();
  
  // First, try current date and 1 day forward (prioritize recent dates)
  for (let daysForward = 0; daysForward <= 1; daysForward++) {
    const targetDate = new Date(currentDate);
    targetDate.setDate(currentDate.getDate() + daysForward);
    
    const year = targetDate.getFullYear();
    const month = targetDate.getMonth() + 1;
    const day = targetDate.getDate();
    
    const datePath = `${year}/${month.toString().padStart(2, '0')}/${day.toString().padStart(2, '0')}`;
    
    possiblePaths.push(`./${datePath}/${justFilename}`);
    possiblePaths.push(`${datePath}/${justFilename}`);
  }
  
  // Then check ALL months (01-12) and ALL days (01-31) for current year - PADDED FORMATS ONLY
  for (let month = 1; month <= 12; month++) {
    for (let day = 1; day <= 31; day++) {
      const monthStr = month.toString().padStart(2, '0');
      const dayStr = day.toString().padStart(2, '0');
      
      const datePath = `${currentYear}/${monthStr}/${dayStr}`;
      
      possiblePaths.push(`./${datePath}/${justFilename}`);
      possiblePaths.push(`${datePath}/${justFilename}`);
    }
  }
  
  // Try alternative date formats for current date only
  const year = currentDate.getFullYear();
  const month = currentDate.getMonth() + 1;
  const day = currentDate.getDate();
  
  // Dashed format for current date
  possiblePaths.push(`./${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}/${justFilename}`);
  possiblePaths.push(`${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}/${justFilename}`);
  
  // Try direct access as fallback
  possiblePaths.push(`./${justFilename}`);
  possiblePaths.push(justFilename);
  
  // Try in root audio/recordings folders
  possiblePaths.push(`./audio/${justFilename}`);
  possiblePaths.push(`./recordings/${justFilename}`);
  possiblePaths.push(`audio/${justFilename}`);
  possiblePaths.push(`recordings/${justFilename}`);
  
  // Remove duplicates while preserving order
  return Array.from(new Set(possiblePaths));
}
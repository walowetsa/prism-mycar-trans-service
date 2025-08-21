import { useState, useEffect } from "react";

interface DateRange {
  start: Date;
  end: Date;
  label: string;
}

interface DateSelectorProps {
  onDateRangeChange: (dateRange: DateRange) => void;
  selectedRange?: DateRange | null;
}

const DateSelector = ({
  onDateRangeChange,
  selectedRange,
}: DateSelectorProps) => {
  const [dateRanges, setDateRanges] = useState<DateRange[]>([]);
  const [selectedIndex, setSelectedIndex] = useState<number | null>(null);
  const [viewMode, setViewMode] = useState<"weekly" | "custom">("weekly");
  const [customStartDate, setCustomStartDate] = useState<string>("");
  const [customEndDate, setCustomEndDate] = useState<string>("");

  // Generate weekly date ranges from January 2025 to current date
  useEffect(() => {
    const generateWeeklyRanges = (): DateRange[] => {
      const ranges: DateRange[] = [];
      const startOfYear = new Date("2025-01-01");
      const currentDate = new Date();

      // Find the first Monday of 2025 (or January 1st if it's a Monday)
      const firstMonday = new Date(startOfYear);
      const dayOfWeek = firstMonday.getDay();
      const daysUntilMonday = dayOfWeek === 0 ? 1 : 8 - dayOfWeek;
      if (dayOfWeek !== 1) {
        firstMonday.setDate(firstMonday.getDate() + daysUntilMonday);
      }

      const currentWeekStart = new Date(firstMonday);

      while (currentWeekStart <= currentDate) {
        const weekEnd = new Date(currentWeekStart);
        weekEnd.setDate(weekEnd.getDate() + 6);
        // FIXED: Set end date to end of day to include all calls on the last day
        weekEnd.setHours(23, 59, 59, 999);

        // Don't include future weeks
        if (currentWeekStart <= currentDate) {
          const formatDate = (date: Date) => {
            return date.toLocaleDateString("en-US", {
              month: "short",
              day: "numeric",
              year:
                date.getFullYear() !== currentDate.getFullYear()
                  ? "numeric"
                  : undefined,
            });
          };

          const label = `${formatDate(currentWeekStart)} - ${formatDate(
            weekEnd
          )}`;

          ranges.push({
            start: new Date(currentWeekStart),
            end: new Date(weekEnd),
            label,
          });
        }

        // Move to next week
        currentWeekStart.setDate(currentWeekStart.getDate() + 7);
      }

      // Reverse to show most recent first
      return ranges.reverse();
    };

    setDateRanges(generateWeeklyRanges());
  }, []);

  // Set initial selection to current week if no selectedRange provided
  useEffect(() => {
    if (dateRanges.length > 0 && selectedRange === undefined && viewMode === "weekly") {
      // Find the current week instead of just using index 0
      const currentWeekIndex = dateRanges.findIndex(range => isCurrentWeek(range));
      const indexToSelect = currentWeekIndex >= 0 ? currentWeekIndex : 0;
      
      setSelectedIndex(indexToSelect);
      onDateRangeChange(dateRanges[indexToSelect]);
    } else if (selectedRange && viewMode === "weekly") {
      // Find matching range if selectedRange is provided
      const matchingIndex = dateRanges.findIndex(
        (range) =>
          range.start.getTime() === selectedRange.start.getTime() &&
          range.end.getTime() === selectedRange.end.getTime()
      );
      setSelectedIndex(matchingIndex >= 0 ? matchingIndex : null);
    }
  }, [dateRanges, selectedRange, onDateRangeChange, viewMode]);

  const handleRangeSelect = (range: DateRange, index: number) => {
    setSelectedIndex(index);
    onDateRangeChange(range);
  };

  const handleCustomDateSubmit = () => {
    if (!customStartDate || !customEndDate) {
      alert("Please select both start and end dates");
      return;
    }

    const startDate = new Date(customStartDate);
    const endDate = new Date(customEndDate);

    // Set start date to beginning of day
    startDate.setHours(0, 0, 0, 0);
    // Set end date to end of day
    endDate.setHours(23, 59, 59, 999);

    if (startDate > endDate) {
      alert("Start date must be before end date");
      return;
    }

    const formatDate = (date: Date) => {
      return date.toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
      });
    };

    const customRange: DateRange = {
      start: startDate,
      end: endDate,
      label: `${formatDate(startDate)} - ${formatDate(endDate)}`,
    };

    // Clear weekly selection when using custom range
    setSelectedIndex(null);
    onDateRangeChange(customRange);
  };

  const handleViewModeChange = (mode: "weekly" | "custom") => {
    setViewMode(mode);
    
    // If switching to weekly and we have a current week, select it
    if (mode === "weekly" && dateRanges.length > 0) {
      const currentWeekIndex = dateRanges.findIndex(range => isCurrentWeek(range));
      const indexToSelect = currentWeekIndex >= 0 ? currentWeekIndex : 0;
      setSelectedIndex(indexToSelect);
      onDateRangeChange(dateRanges[indexToSelect]);
    }
  };

  const isCurrentWeek = (range: DateRange): boolean => {
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const rangeStart = new Date(
      range.start.getFullYear(),
      range.start.getMonth(),
      range.start.getDate()
    );
    const rangeEnd = new Date(
      range.end.getFullYear(),
      range.end.getMonth(),
      range.end.getDate()
    );

    return today >= rangeStart && today <= rangeEnd;
  };

  // Format date for input fields (YYYY-MM-DD)
  const formatDateForInput = (date: Date): string => {
    return date.toISOString().split('T')[0];
  };

  // Set default custom dates to current week
  useEffect(() => {
    if (dateRanges.length > 0) {
      const currentWeekIndex = dateRanges.findIndex(range => isCurrentWeek(range));
      const currentWeek = currentWeekIndex >= 0 ? dateRanges[currentWeekIndex] : dateRanges[0];
      
      if (currentWeek) {
        setCustomStartDate(formatDateForInput(currentWeek.start));
        setCustomEndDate(formatDateForInput(currentWeek.end));
      }
    }
  }, [dateRanges]);

  return (
    <div className="border-2 p-2 rounded border-border bg-bg-secondary text-text-primary">
      <h5 className="text-[#4ecca3] mb-3">Date Selection</h5>
      
      {/* View Mode Toggle */}
      <div className="mb-4">
        <div className="flex rounded-lg bg-bg-primary p-1 gap-1">
          <button
            onClick={() => handleViewModeChange("weekly")}
            className={`flex-1 px-3 py-2 text-sm rounded-md transition-colors duration-150 ${
              viewMode === "weekly"
                ? "bg-[#4ecca3] text-[#0a101b] font-semibold"
                : "text-gray-400 hover:text-white"
            }`}
          >
            Weekly
          </button>
          <button
            onClick={() => handleViewModeChange("custom")}
            className={`flex-1 px-3 py-2 text-sm rounded-md transition-colors duration-150 ${
              viewMode === "custom"
                ? "bg-[#4ecca3] text-[#0a101b] font-semibold"
                : "text-gray-400 hover:text-white"
            }`}
          >
            Custom
          </button>
        </div>
      </div>

      {/* Weekly View */}
      {viewMode === "weekly" && (
        <ul
          className="max-h-[calc(100vh-200px)] overflow-y-auto [&::-webkit-scrollbar]:w-2
    [&::-webkit-scrollbar-track]:rounded-full
    [&::-webkit-scrollbar-track]:bg-gray-100
    [&::-webkit-scrollbar-thumb]:rounded-full
    [&::-webkit-scrollbar-thumb]:bg-gray-300
    dark:[&::-webkit-scrollbar-track]:bg-neutral-700
    dark:[&::-webkit-scrollbar-thumb]:bg-neutral-500"
        >
          {dateRanges.map((range, index) => (
            <li key={`${range.start.getTime()}-${range.end.getTime()}`}>
              <button
                onClick={() => handleRangeSelect(range, index)}
                className={`
                  w-full text-left px-3 py-2 rounded-md transition-colors duration-150
                  hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-1
                  ${
                    selectedIndex === index
                      ? "bg-blue-100 text-blue-900 border border-blue-300"
                      : "text-gray-700 border border-transparent"
                  }
                  ${isCurrentWeek(range) ? "font-semibold" : "font-normal"}
                `}
              >
                <div className="flex items-center justify-between">
                  <span>{range.label}</span>
                  {isCurrentWeek(range) && (
                    <span className="text-xs bg-green-100 text-green-800 px-2 py-1 rounded-full">
                      Current
                    </span>
                  )}
                </div>
              </button>
            </li>
          ))}
        </ul>
      )}

      {/* Custom Date Range View */}
      {viewMode === "custom" && (
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">
              Start Date
            </label>
            <input
              type="date"
              value={customStartDate}
              onChange={(e) => setCustomStartDate(e.target.value)}
              className="w-full px-3 py-2 bg-bg-primary border border-border rounded-md text-white focus:outline-none focus:ring-2 focus:ring-[#4ecca3] focus:border-transparent"
              max={formatDateForInput(new Date())} // Don't allow future dates
            />
          </div>
          
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">
              End Date
            </label>
            <input
              type="date"
              value={customEndDate}
              onChange={(e) => setCustomEndDate(e.target.value)}
              className="w-full px-3 py-2 bg-bg-primary border border-border rounded-md text-white focus:outline-none focus:ring-2 focus:ring-[#4ecca3] focus:border-transparent"
              min={customStartDate} // End date must be after start date
              max={formatDateForInput(new Date())} // Don't allow future dates
            />
          </div>
          
          <button
            onClick={handleCustomDateSubmit}
            className="w-full px-4 py-2 bg-[#4ecca3] text-[#0a101b] rounded-md hover:bg-[#3bb891] transition-colors font-medium focus:outline-none focus:ring-2 focus:ring-[#4ecca3] focus:ring-offset-2"
          >
            Apply Date Range
          </button>

          {/* Quick Date Range Buttons */}
          <div className="space-y-2">
            <div className="text-sm font-medium text-gray-300 mb-2">Quick Select:</div>
            <div className="grid grid-cols-2 gap-2">
              <button
                onClick={() => {
                  const today = new Date();
                  setCustomStartDate(formatDateForInput(today));
                  setCustomEndDate(formatDateForInput(today));
                }}
                className="px-3 py-1 text-xs bg-bg-primary border border-border rounded text-gray-300 hover:bg-gray-600 transition-colors"
              >
                Today
              </button>
              <button
                onClick={() => {
                  const today = new Date();
                  const yesterday = new Date(today);
                  yesterday.setDate(today.getDate() - 1);
                  setCustomStartDate(formatDateForInput(yesterday));
                  setCustomEndDate(formatDateForInput(yesterday));
                }}
                className="px-3 py-1 text-xs bg-bg-primary border border-border rounded text-gray-300 hover:bg-gray-600 transition-colors"
              >
                Yesterday
              </button>
              <button
                onClick={() => {
                  const today = new Date();
                  const lastWeek = new Date(today);
                  lastWeek.setDate(today.getDate() - 7);
                  setCustomStartDate(formatDateForInput(lastWeek));
                  setCustomEndDate(formatDateForInput(today));
                }}
                className="px-3 py-1 text-xs bg-bg-primary border border-border rounded text-gray-300 hover:bg-gray-600 transition-colors"
              >
                Last 7 Days
              </button>
              <button
                onClick={() => {
                  const today = new Date();
                  const lastMonth = new Date(today);
                  lastMonth.setDate(today.getDate() - 30);
                  setCustomStartDate(formatDateForInput(lastMonth));
                  setCustomEndDate(formatDateForInput(today));
                }}
                className="px-3 py-1 text-xs bg-bg-primary border border-border rounded text-gray-300 hover:bg-gray-600 transition-colors"
              >
                Last 30 Days
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default DateSelector;
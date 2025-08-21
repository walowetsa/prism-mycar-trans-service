"use client";
import { useState, useEffect } from "react";
import DateSelector from "../ui/DateSelector";
import CallLogDisplay from "../layouts/CallLogDisplay";

interface DateRange {
  start: Date;
  end: Date;
  label: string;
}

const TGEDash = () => {
  const [selectedDateRange, setSelectedDateRange] = useState<DateRange | null>(null);

  // Generate current week date range on component mount
  useEffect(() => {
    const getCurrentWeekRange = (): DateRange => {
      const now = new Date();
      const currentDay = now.getDay(); // 0 = Sunday, 1 = Monday, etc.
      
      // Calculate days to subtract to get to Monday (start of week)
      const daysToMonday = currentDay === 0 ? 6 : currentDay - 1;
      
      // Get Monday of current week
      const monday = new Date(now);
      monday.setDate(now.getDate() - daysToMonday);
      monday.setHours(0, 0, 0, 0); // Set to start of day
      
      // Get Sunday of current week (6 days after Monday)
      const sunday = new Date(monday);
      sunday.setDate(monday.getDate() + 6);
      sunday.setHours(23, 59, 59, 999); // Set to end of day
      
      // Format the label
      const formatDate = (date: Date) => {
        return date.toLocaleDateString("en-US", {
          month: "short",
          day: "numeric",
          year: date.getFullYear() !== now.getFullYear() ? "numeric" : undefined,
        });
      };
      
      const label = `${formatDate(monday)} - ${formatDate(sunday)}`;
      
      return {
        start: monday,
        end: sunday,
        label,
      };
    };

    // Set the current week as default
    const currentWeek = getCurrentWeekRange();
    setSelectedDateRange(currentWeek);
  }, []);

  const handleDateRangeChange = (dateRange: DateRange) => {
    setSelectedDateRange(dateRange);
  };

  return (
    <div className="flex flex-1 p-4 gap-x-4">
      <DateSelector 
        onDateRangeChange={handleDateRangeChange}
        selectedRange={selectedDateRange}
      />
      <CallLogDisplay 
        selectedDateRange={selectedDateRange}
      />
    </div>
  );
};

export default TGEDash;